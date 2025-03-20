#include "nbd_session.h"

#include "bootstrap.h"
#include "location.h"
#include "nbd_chunk_handler.h"

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/tools.h>

#include <util/system/types.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NTools;

////////////////////////////////////////////////////////////////////////////////

TNbdSession::TNbdSession(
    TDataNodeConfigPtr config,
    IBootstrap* bootstrap,
    TSessionId sessionId,
    TSessionOptions options,
    TStoreLocationPtr storeLocation,
    NConcurrency::TLease lease,
    TLockedChunkGuard lockedChunkGuard)
    : Config_(std::move(config))
    , Bootstrap_(std::move(bootstrap))
    , Id_(std::move(sessionId))
    , Options_(std::move(options))
    , StoreLocation_(std::move(storeLocation))
    , Lease_(std::move(lease))
    , LockedChunkGuard_(std::move(lockedChunkGuard))
    , StartTime_(TInstant::Now())
{
    YT_VERIFY(Options_.NbdChunkFsType);
    YT_VERIFY(Options_.NbdChunkSize);

    NbdChunkHandler_ = CreateNbdChunkHandler(
        *Options_.NbdChunkSize,
        Id_.ChunkId,
        Options_.WorkloadDescriptor,
        StoreLocation_,
        Bootstrap_->GetStorageHeavyInvoker());
}

/*
    NBD specific calls.
*/

TFuture<TBlock> TNbdSession::Read(i64 offset, i64 length)
{
    // We are reading out some bytes to network so use out throttler.
    auto readThrottler = Bootstrap_->GetOutThrottler(Options_.WorkloadDescriptor);
    auto throttleFuture = readThrottler->Throttle(length);
    return throttleFuture.Apply(BIND([=, this, this_ = MakeStrong(this)] () {
        return NbdChunkHandler_->Read(offset, length);
    }));
}

TFuture<NIO::TIOCounters> TNbdSession::Write(i64 offset, const TBlock& block)
{
    // We are writing in some bytes from network so use in throttler.
    auto writeThrottler = Bootstrap_->GetInThrottler(Options_.WorkloadDescriptor);
    auto throttleFuture = writeThrottler->Throttle(block.Size());
    return throttleFuture.Apply(BIND([=, this, this_ = MakeStrong(this)] () {
        return NbdChunkHandler_->Write(offset, block);
    }));
}

//! Create NBD chunk and make filesystem on it.
TFuture<void> TNbdSession::Create()
{
    return NbdChunkHandler_->Create().Apply(BIND([this, this_ = MakeStrong(this)] () {
        // NB. Filesystem is made directly bypassing io engine.
        auto config = New<TMkFsConfig>();
        config->Path = StoreLocation_->GetChunkPath(Id_.ChunkId);
        config->Type = ToString(*Options_.NbdChunkFsType);
        try {
            RunTool<TMkFsAsRootTool>(config);
        } catch (const std::exception&) {
            std::ignore = WaitFor(NbdChunkHandler_->Destroy());
            throw;
        }
    })
    .AsyncVia(Bootstrap_->GetStorageHeavyInvoker()));
}

//! Remove NBD chunk.
TFuture<void> TNbdSession::Destroy()
{
    // Remove NBD chunk first.
    return NbdChunkHandler_->Destroy().Apply(BIND([this, this_ = MakeStrong(this)] {
        TLeaseManager::CloseLease(Lease_);

        // Unlock NBD chunk and unregister session.
        Finished_.Fire(Error_);
    })
    .AsyncVia(Bootstrap_->GetStorageHeavyInvoker()));
}

/*
    Session calls.
*/

TChunkId TNbdSession::GetChunkId() const&
{
    return Id_.ChunkId;
}

TSessionId TNbdSession::GetId() const&
{
    return Id_;
}

ESessionType TNbdSession::GetType() const
{
    return ESessionType::Nbd;
}

NClusterNode::TMasterEpoch TNbdSession::GetMasterEpoch() const
{
    THROW_ERROR_EXCEPTION("Not implemented");
}

const TWorkloadDescriptor& TNbdSession::GetWorkloadDescriptor() const
{
    return Options_.WorkloadDescriptor;
}

const TSessionOptions& TNbdSession::GetSessionOptions() const
{
    return Options_;
}

const TStoreLocationPtr& TNbdSession::GetStoreLocation() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return StoreLocation_;
}

TFuture<void> TNbdSession::Start()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Create();
}

void TNbdSession::Cancel(const TError& error)
{
    Error_ = error;
    WaitFor(Destroy()).ThrowOnError();
}

TInstant TNbdSession::GetStartTime() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return StartTime_;
}

i64 TNbdSession::GetMemoryUsage() const
{
    return 0;
}

i64 TNbdSession::GetTotalSize() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Options_.NbdChunkSize.value_or(0);
}

i64 TNbdSession::GetBlockCount() const
{
    return 0;
}

i64 TNbdSession::GetWindowSize() const
{
    return 0;
}

i64 TNbdSession::GetIntermediateEmptyBlockCount() const
{
    return 0;
}

TFuture<ISession::TFinishResult> TNbdSession::Finish(
    const NChunkClient::TRefCountedChunkMetaPtr& /* chunkMeta */,
    std::optional<int> /* blockCount */,
    bool /*truncateExtraBlocks*/)
{
    THROW_ERROR_EXCEPTION("Not implemented");
}

TFuture<NIO::TIOCounters> TNbdSession::PutBlocks(
    int /* startBlockIndex */,
    std::vector<NChunkClient::TBlock> /* blocks */,
    i64 /* cumulativeBlockSize */,
    bool /* enableCaching */)
{
    THROW_ERROR_EXCEPTION("Not implemented");
}

TFuture<NChunkClient::TDataNodeServiceProxy::TRspPutBlocksPtr> TNbdSession::SendBlocks(
    int /* startBlockIndex */,
    int /* blockCount */,
    i64 /* cumulativeBlockSize */,
    const NNodeTrackerClient::TNodeDescriptor& /* target */)
{
    THROW_ERROR_EXCEPTION("Not implemented");
}

TFuture<ISession::TFlushBlocksResult> TNbdSession::FlushBlocks(int /* blockIndex */)
{
    THROW_ERROR_EXCEPTION("Not implemented");
}

void TNbdSession::Ping()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    TLeaseManager::RenewLease(Lease_);
}

void TNbdSession::OnUnregistered()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    UnregisteredEvent_.Set();
}

void TNbdSession::UnlockChunk()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = std::move(LockedChunkGuard_);
}

TFuture<void> TNbdSession::GetUnregisteredEvent()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return UnregisteredEvent_.ToFuture();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
