#include "session_detail.h"
#include "private.h"
#include "config.h"
#include "location.h"
#include "master_connector.h"
#include "session_manager.h"

#include <yt/server/cell_node/bootstrap.h>

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
    const TSessionId& sessionId,
    const TSessionOptions& options,
    TStoreLocationPtr location,
    TLease lease)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
    , SessionId_(sessionId)
    , Options_(options)
    , Location_(location)
    , Lease_(std::move(lease))
    , WriteInvoker_(CreateSerializedInvoker(Location_->GetWritePoolInvoker()))
    , Logger(NLogging::TLogger(DataNodeLogger)
        .AddTag("LocationId: %v, ChunkId: %v",
            Location_->GetId(),
            SessionId_))
    , Profiler(location->GetProfiler())
{
    YCHECK(Bootstrap_);
    YCHECK(Location_);
    YCHECK(Lease_);
    VERIFY_THREAD_AFFINITY(ControlThread);
}

const TChunkId& TSessionBase::GetChunkId() const&
{
    return SessionId_.ChunkId;
}

const TSessionId& TSessionBase::GetId() const&
{
    return SessionId_;
}

ESessionType TSessionBase::GetType() const
{
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
    return Options_.WorkloadDescriptor;
}

TStoreLocationPtr TSessionBase::GetStoreLocation() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Location_;
}

TFuture<void> TSessionBase::Start()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YT_LOG_DEBUG("Starting session");

    return DoStart().Apply(BIND([=, this_ = MakeStrong(this)] {
        YCHECK(!Active_);
        Active_ = true;

        YT_LOG_DEBUG("Session started");
    }).AsyncVia(Bootstrap_->GetControlInvoker()));
}

void TSessionBase::Ping()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // Let's be generous and accept pings in any state.
    if (Lease_) {
        TLeaseManager::RenewLease(Lease_);
    }
}

void TSessionBase::Cancel(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!Active_) {
        return;
    }

    YT_LOG_DEBUG(error, "Canceling session");

    TLeaseManager::CloseLease(Lease_);
    Active_ = false;
    Canceled_.store(true);

    DoCancel(error);
}

TFuture<IChunkPtr> TSessionBase::Finish(const TRefCountedChunkMetaPtr& chunkMeta, std::optional<int> blockCount)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    try {
        ValidateActive();

        YT_LOG_INFO("Finishing session");

        TLeaseManager::CloseLease(Lease_);
        Active_ = false;

        return DoFinish(chunkMeta, blockCount);
    } catch (const std::exception& ex) {
        return MakeFuture<IChunkPtr>(ex);
    }
}

TFuture<void> TSessionBase::PutBlocks(
    int startBlockIndex,
    const std::vector<TBlock>& blocks,
    bool enableCaching)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    try {
        ValidateActive();
        Ping();

        return DoPutBlocks(startBlockIndex, blocks, enableCaching);
    } catch (const std::exception& ex) {
        return MakeFuture<void>(ex);
    }
}

TFuture<TDataNodeServiceProxy::TRspPutBlocksPtr> TSessionBase::SendBlocks(
    int startBlockIndex,
    int blockCount,
    const TNodeDescriptor& targetDescriptor)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    try {
        ValidateActive();
        Ping();

        return DoSendBlocks(startBlockIndex, blockCount, targetDescriptor);
    } catch (const std::exception& ex) {
        return MakeFuture<TDataNodeServiceProxy::TRspPutBlocksPtr>(ex);
    }
}

TFuture<void> TSessionBase::FlushBlocks(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    try {
        ValidateActive();
        Ping();

        return DoFlushBlocks(blockIndex);
    } catch (const std::exception& ex) {
        return MakeFuture<void>(ex);
    }
}

void TSessionBase::ValidateActive() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!Active_) {
        THROW_ERROR_EXCEPTION("Session is not active");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
