#include "session_detail.h"
#include "private.h"
#include "config.h"
#include "location.h"
#include "master_connector.h"
#include "session_manager.h"

#include <yt/server/node/cell_node/bootstrap.h>

#include <yt/core/profiling/timing.h>

namespace NYT::NDataNode {

using namespace NRpc;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NCellNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TSessionBase::TSessionBase(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap,
    TSessionId sessionId,
    const TSessionOptions& options,
    TStoreLocationPtr location,
    TLease lease)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
    , SessionId_(sessionId)
    , Options_(options)
    , Location_(location)
    , Lease_(std::move(lease))
    , SessionInvoker_(CreateBoundedConcurrencyInvoker(Location_->GetWritePoolInvoker(), 1))
    , Logger(NLogging::TLogger(DataNodeLogger)
        .AddTag("LocationId: %v, ChunkId: %v",
            Location_->GetId(),
            SessionId_))
    , Profiler(location->GetProfiler())
{
    YT_VERIFY(Bootstrap_);
    YT_VERIFY(Location_);
    YT_VERIFY(Lease_);
}

TChunkId TSessionBase::GetChunkId() const&
{
    VERIFY_THREAD_AFFINITY_ANY();

    return SessionId_.ChunkId;
}

TSessionId TSessionBase::GetId() const&
{
    VERIFY_THREAD_AFFINITY_ANY();

    return SessionId_;
}

ESessionType TSessionBase::GetType() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    switch (Options_.WorkloadDescriptor.Category) {
        case EWorkloadCategory::SystemRepair:
            return ESessionType::Repair;
        case EWorkloadCategory::SystemReplication:
            return ESessionType::Replication;
        default:
            return ESessionType::User;
    }
}

const TWorkloadDescriptor& TSessionBase::GetWorkloadDescriptor() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Options_.WorkloadDescriptor;
}

const TStoreLocationPtr& TSessionBase::GetStoreLocation() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Location_;
}

TFuture<void> TSessionBase::Start()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Starting session");

    return
        BIND(&TSessionBase::DoStart, MakeStrong(this))
            .AsyncVia(SessionInvoker_)
            .Run()
            .Apply(BIND([=, this_ = MakeStrong(this)] (const TError& error) {
                VERIFY_INVOKER_AFFINITY(SessionInvoker_);

                YT_VERIFY(!Active_);
                Active_ = true;

                if (!error.IsOK()) {
                    YT_LOG_DEBUG(error, "Session has failed to start");
                    Cancel(error);
                    THROW_ERROR(error);
                }

                YT_LOG_DEBUG("Session started");

                if (!PendingCancelationError_.IsOK()) {
                    Cancel(PendingCancelationError_);
                }
            }).AsyncVia(SessionInvoker_))
            // TODO(babenko): session start cancelation is not properly supported
            .ToUncancelable();
}

void TSessionBase::Ping()
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Let's be generous and accept pings in any state.
    TLeaseManager::RenewLease(Lease_);
}

void TSessionBase::Cancel(const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YT_VERIFY(!error.IsOK());

    SessionInvoker_->Invoke(
        BIND([=, this_ = MakeStrong(this)] {
            VERIFY_INVOKER_AFFINITY(SessionInvoker_);

            if (!Active_) {
                YT_LOG_DEBUG(error, "Session will be canceled after becoming active");
                PendingCancelationError_ = error;
                return;
            }

            YT_LOG_DEBUG(error, "Canceling session");

            TLeaseManager::CloseLease(Lease_);
            Active_ = false;
            Canceled_.store(true);

            DoCancel(error);
        }));
}

TFuture<NChunkClient::NProto::TChunkInfo> TSessionBase::Finish(
    const TRefCountedChunkMetaPtr& chunkMeta,
    std::optional<int> blockCount)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return
        BIND([=, this_ = MakeStrong(this)] {
            VERIFY_INVOKER_AFFINITY(SessionInvoker_);

            ValidateActive();

            YT_LOG_DEBUG("Finishing session");

            TLeaseManager::CloseLease(Lease_);
            Active_ = false;

            return DoFinish(chunkMeta, blockCount);
        })
        .AsyncVia(SessionInvoker_)
        .Run();
}

TFuture<void> TSessionBase::PutBlocks(
    int startBlockIndex,
    const std::vector<TBlock>& blocks,
    bool enableCaching)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return
        BIND([=, this_ = MakeStrong(this)] {
            VERIFY_INVOKER_AFFINITY(SessionInvoker_);

            ValidateActive();
            Ping();

            return DoPutBlocks(startBlockIndex, blocks, enableCaching);
        })
        .AsyncVia(SessionInvoker_)
        .Run();
}

TFuture<TDataNodeServiceProxy::TRspPutBlocksPtr> TSessionBase::SendBlocks(
    int startBlockIndex,
    int blockCount,
    const TNodeDescriptor& targetDescriptor)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return
        BIND([=, this_ = MakeStrong(this)] {
            VERIFY_INVOKER_AFFINITY(SessionInvoker_);

            ValidateActive();
            Ping();

            return DoSendBlocks(startBlockIndex, blockCount, targetDescriptor);
        })
        .AsyncVia(SessionInvoker_)
        .Run();
}

TFuture<void> TSessionBase::FlushBlocks(int blockIndex)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return
        BIND([=, this_ = MakeStrong(this)] {
            VERIFY_INVOKER_AFFINITY(SessionInvoker_);
            
            ValidateActive();
            Ping();

            return DoFlushBlocks(blockIndex);
        })
        .AsyncVia(SessionInvoker_)
        .Run();
}

void TSessionBase::ValidateActive() const
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    if (!Active_) {
        THROW_ERROR_EXCEPTION("Session is not active");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
