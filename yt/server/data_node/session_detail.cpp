#include "session_detail.h"
#include "private.h"
#include "config.h"
#include "location.h"
#include "master_connector.h"
#include "session_manager.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/core/profiling/timing.h>

namespace NYT {
namespace NDataNode {

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
    : Config_(config)
    , Bootstrap_(bootstrap)
    , SessionId_(sessionId)
    , Options_(options)
    , Location_(location)
    , Lease_(lease)
    , WriteInvoker_(CreateSerializedInvoker(
        CreateFixedPriorityInvoker(
            Location_->GetWritePoolInvoker(),
            options.WorkloadDescriptor.GetPriority())))
    , Logger(DataNodeLogger)
    , Profiler(location->GetProfiler())
{
    YCHECK(bootstrap);
    YCHECK(location);
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetControlInvoker(), ControlThread);

    Logger.AddTag("LocationId: %v, ChunkId: %v",
        Location_->GetId(),
        SessionId_);

    Location_->UpdateSessionCount(GetType(), +1);
}

TSessionBase::~TSessionBase()
{
    Location_->UpdateSessionCount(GetType(), -1);
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

    LOG_DEBUG("Starting session");

    return DoStart().Apply(BIND([=, this_ = MakeStrong(this)] {
        YCHECK(!Active_);
        Active_ = true;

        LOG_DEBUG("Session started");
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

    if (!Active_)
        return;

    LOG_INFO(error, "Canceling session");

    TLeaseManager::CloseLease(Lease_);
    Active_ = false;

    DoCancel();
}

TFuture<IChunkPtr> TSessionBase::Finish(const TChunkMeta* chunkMeta, const TNullable<int>& blockCount)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    try {
        ValidateActive();

        LOG_INFO("Finishing session");
    
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

TFuture<void> TSessionBase::SendBlocks(
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
        return MakeFuture<void>(ex);
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

void TSessionBase::ValidateActive()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!Active_) {
        THROW_ERROR_EXCEPTION("Session is not active");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
