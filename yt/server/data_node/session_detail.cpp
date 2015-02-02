#include "stdafx.h"
#include "session_detail.h"
#include "session_manager.h"
#include "private.h"
#include "config.h"
#include "location.h"

#include <core/profiling/scoped_timer.h>

#include <server/cell_node/bootstrap.h>

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
    const TChunkId& chunkId,
    const TSessionOptions& options,
    TLocationPtr location,
    TLease lease)
    : Config_(config)
    , Bootstrap_(bootstrap)
    , ChunkId_(chunkId)
    , Options_(options)
    , Location_(location)
    , Lease_(lease)
    , WriteInvoker_(CreateSerializedInvoker(Location_->GetWritePoolInvoker()))
    , Logger(DataNodeLogger)
    , Profiler(location->Profiler())
{
    YCHECK(bootstrap);
    YCHECK(location);

    Logger.AddTag("LocationId: %v, ChunkId: %v",
        Location_->GetId(),
        ChunkId_);

    Location_->UpdateSessionCount(+1);
}

TSessionBase::~TSessionBase()
{
    Location_->UpdateSessionCount(-1);
}

const TChunkId& TSessionBase::GetChunkId() const
{
    return ChunkId_;
}

EWriteSessionType TSessionBase::GetType() const
{
    return Options_.SessionType;
}

TLocationPtr TSessionBase::GetLocation() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Location_;
}

TFuture<void> TSessionBase::Start()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_DEBUG("Session started");

    YCHECK(!Active_);
    Active_ = true;

    return DoStart();
}

void TSessionBase::Ping()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ValidateActive();

    TLeaseManager::RenewLease(Lease_);
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

TFuture<IChunkPtr> TSessionBase::Finish(const TChunkMeta& chunkMeta, const TNullable<int>& blockCount)
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
    const std::vector<TSharedRef>& blocks,
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
    const TNodeDescriptor& target)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    try {
        ValidateActive();
        Ping();

        return DoSendBlocks(startBlockIndex, blockCount, target);
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
