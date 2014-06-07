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

static auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TSessionBase::TSessionBase(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap,
    const TChunkId& chunkId,
    NChunkClient::EWriteSessionType type,
    bool syncOnClose,
    TLocationPtr location)
    : Config_(config)
    , Bootstrap_(bootstrap)
    , ChunkId_(chunkId)
    , Type_(type)
    , SyncOnClose_(syncOnClose)
    , Location_(location)
    , WriteInvoker_(CreateSerializedInvoker(Location_->GetWriteInvoker()))
    , Logger(DataNodeLogger)
    , Profiler(location->Profiler())
{
    YCHECK(bootstrap);
    YCHECK(location);

    Logger.AddTag(Sprintf("LocationId: %s, ChunkId: %s",
        ~Location_->GetId(),
        ~ToString(ChunkId_)));

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
    return Type_;
}

TLocationPtr TSessionBase::GetLocation() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Location_;
}

void TSessionBase::Start(TLeaseManager::TLease lease)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_DEBUG("Session started (SessionType: %s)",
        ~ToString(Type_));

    Lease_ = lease;

    YCHECK(!Active_);
    Active_ = true;

    DoStart();
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

    ValidateActive();

    LOG_INFO(error, "Canceling session");

    TLeaseManager::CloseLease(Lease_);
    Active_ = false;

    DoCancel();
}

TFuture<TErrorOr<IChunkPtr>> TSessionBase::Finish(const TChunkMeta& chunkMeta)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ValidateActive();

    LOG_INFO("Finishing session");
    
    TLeaseManager::CloseLease(Lease_);
    Active_ = false;

    return DoFinish(chunkMeta);
}

TAsyncError TSessionBase::PutBlocks(
    int startBlockIndex,
    const std::vector<TSharedRef>& blocks,
    bool enableCaching)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ValidateActive();
    Ping();

    return DoPutBlocks(startBlockIndex, blocks, enableCaching);
}

TAsyncError TSessionBase::SendBlocks(
    int startBlockIndex,
    int blockCount,
    const TNodeDescriptor& target)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ValidateActive();
    Ping();

    return DoSendBlocks(startBlockIndex, blockCount, target);
}

TAsyncError TSessionBase::FlushBlocks(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ValidateActive();
    Ping();

    return DoFlushBlocks(blockIndex);
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
