#include "stdafx.h"
#include "blob_session.h"
#include "session_manager.h"
#include "private.h"
#include "config.h"
#include "location.h"
#include "block_store.h"
#include "blob_chunk.h"
#include "chunk_store.h"

#include <core/misc/fs.h>
#include <core/misc/sync.h>

#include <ytlib/chunk_client/file_writer.h>
#include <ytlib/chunk_client/chunk_meta.pb.h>

#include <ytlib/node_tracker_client/node_directory.h>

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
static NProfiling::TRateCounter DiskWriteThroughputCounter("/disk_write_throughput_counter");

////////////////////////////////////////////////////////////////////////////////

TBlobSession::TBlobSession(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap,
    const TChunkId& chunkId,
    EWriteSessionType type,
    bool syncOnClose,
    TLocationPtr location)
    : TSession(
        config,
        bootstrap,
        chunkId,
        type,
        syncOnClose,
        location)
    , WindowStartIndex_(0)
    , WriteIndex_(0)
    , Size_(0)
{ }

void TBlobSession::Start(TLeaseManager::TLease lease)
{
    TSession::Start(lease);

    WriteInvoker_->Invoke(
        BIND(&TBlobSession::DoOpenWriter, MakeStrong(this)));
}

void TBlobSession::DoOpenWriter()
{
    LOG_DEBUG("Started opening blob chunk writer");

    PROFILE_TIMING ("/blob_chunk_open_time") {
        try {
            auto fileName = Location_->GetChunkFileName(ChunkId_);
            Writer_ = New<TFileWriter>(fileName, SyncOnClose_);
            Writer_->Open();
        }
        catch (const std::exception& ex) {
            OnIOError(TError(
                NChunkClient::EErrorCode::IOError,
                "Error creating chunk %s",
                ~ToString(ChunkId_))
                << ex);
            return;
        }
    }

    LOG_DEBUG("Finished opening blob chunk writer");
}

TFuture<TErrorOr<IChunkPtr>> TBlobSession::Finish(const TChunkMeta& chunkMeta)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    CloseLease();

    for (int blockIndex = WindowStartIndex_; blockIndex < Window_.size(); ++blockIndex) {
        const auto& slot = GetSlot(blockIndex);
        if (slot.State != ESlotState::Empty) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::WindowError,
                "Attempt to finish a session with an unflushed block %d",
                blockIndex)
                << TErrorAttribute("window_start", WindowStartIndex_)
                << TErrorAttribute("window_size", Window_.size());
        }
    }

    LOG_DEBUG("Finishing session");

    return CloseWriter(chunkMeta).Apply(
        BIND(&TBlobSession::OnWriterClosed, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetControlInvoker()));
}

const TChunkInfo& TBlobSession::GetChunkInfo() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Writer_->GetChunkInfo();
}

TAsyncError TBlobSession::PutBlocks(
    int startBlockIndex,
    const std::vector<TSharedRef>& blocks,
    bool enableCaching)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Ping();

    auto blockStore = Bootstrap_->GetBlockStore();

    int blockIndex = startBlockIndex;
    i64 requestSize = 0;

    for (const auto& block : blocks) {
        TBlockId blockId(ChunkId_, blockIndex);
        ValidateBlockIsInWindow(blockIndex);

        if (!Location_->HasEnoughSpace(block.Size())) {
            return MakeFuture(TError(
                NChunkClient::EErrorCode::OutOfSpace,
                "No enough space left on node"));
        }

        auto& slot = GetSlot(blockIndex);
        if (slot.State != ESlotState::Empty) {
            if (TRef::AreBitwiseEqual(slot.Block, block)) {
                LOG_WARNING("Block is already received (Block: %d)", blockIndex);
                continue;
            }

            return MakeFuture(TError(
                NChunkClient::EErrorCode::BlockContentMismatch,
                "Block %d with a different content already received",
                blockIndex)
                << TErrorAttribute("window_start", WindowStartIndex_));
        }

        slot.State = ESlotState::Received;
        slot.Block = block;

        if (enableCaching) {
            blockStore->PutBlock(blockId, block, Null);
        }

        Location_->UpdateUsedSpace(block.Size());
        Size_ += block.Size();
        requestSize += block.Size();

        LOG_DEBUG("Chunk block received (Block: %d)", blockIndex);

        ++blockIndex;
    }

    EnqueueWrites();

    auto throttler = Bootstrap_->GetInThrottler(Type_);
    return throttler->Throttle(requestSize).Apply(BIND([] () {
        return TError();
    }));
}

TAsyncError TBlobSession::SendBlocks(
    int startBlockIndex,
    int blockCount,
    const TNodeDescriptor& target)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Ping();

    TDataNodeServiceProxy proxy(ChannelFactory->CreateChannel(target.Address));
    proxy.SetDefaultTimeout(Config_->NodeRpcTimeout);

    auto req = proxy.PutBlocks();
    ToProto(req->mutable_chunk_id(), ChunkId_);
    req->set_start_block_index(startBlockIndex);

    i64 requestSize = 0;
    for (int blockIndex = startBlockIndex; blockIndex < startBlockIndex + blockCount; ++blockIndex) {
        auto block = GetBlock(blockIndex);
        req->Attachments().push_back(block);
        requestSize += block.Size();
    }

    auto throttler = Bootstrap_->GetOutThrottler(Type_);
    return throttler->Throttle(requestSize).Apply(BIND([=] () {
        return req->Invoke().Apply(BIND([=] (TDataNodeServiceProxy::TRspPutBlocksPtr rsp) {
            return rsp->GetError();
        }));
    }));
}

void TBlobSession::EnqueueWrites()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto sessionManager = Bootstrap_->GetSessionManager();
    while (WriteIndex_ < Window_.size()) {
        const auto& slot = GetSlot(WriteIndex_);
        YCHECK(slot.State == ESlotState::Received || slot.State == ESlotState::Empty);
        if (slot.State == ESlotState::Empty)
            break;

        sessionManager->UpdatePendingWriteSize(slot.Block.Size());

        BIND(
            &TBlobSession::DoWriteBlock,
            MakeStrong(this),
            slot.Block,
            WriteIndex_)
        .AsyncVia(WriteInvoker_)
        .Run()
        .Subscribe(
            BIND(&TBlobSession::OnBlockWritten, MakeStrong(this), WriteIndex_)
                .Via(Bootstrap_->GetControlInvoker()));
        ++WriteIndex_;
    }
}

TError TBlobSession::DoWriteBlock(const TSharedRef& block, int blockIndex)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!Error_.IsOK()) {
        return Error_;
    }

    LOG_DEBUG("Started writing block %d (BlockSize: %" PRISZT ")",
        blockIndex,
        block.Size());

    TScopedTimer timer;
    try {
        if (!Writer_->WriteBlock(block)) {
            auto result = Writer_->GetReadyEvent().Get();
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
            YUNREACHABLE();
        }
    } catch (const std::exception& ex) {
        TBlockId blockId(ChunkId_, blockIndex);
        OnIOError(TError(
            NChunkClient::EErrorCode::IOError,
            "Error writing chunk block %s",
            ~ToString(blockId))
            << ex);
    }

    auto writeTime = timer.GetElapsed();

    LOG_DEBUG("Finished writing block %d", blockIndex);

    auto& locationProfiler = Location_->Profiler();
    locationProfiler.Enqueue("/blob_block_write_size", block.Size());
    locationProfiler.Enqueue("/blob_block_write_time", writeTime.MicroSeconds());
    locationProfiler.Enqueue("/blob_block_write_speed", block.Size() * 1000000 / (1 + writeTime.MicroSeconds()));

    DataNodeProfiler.Increment(DiskWriteThroughputCounter, block.Size());

    return Error_;
}

void TBlobSession::OnBlockWritten(int blockIndex, TError error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    UNUSED(error);

    if (!Error_.IsOK()) {
        return;
    }

    auto& slot = GetSlot(blockIndex);
    YCHECK(slot.State == ESlotState::Received);
    slot.State = ESlotState::Written;
    slot.IsWritten.Set();

    auto sessionManager = Bootstrap_->GetSessionManager();
    sessionManager->UpdatePendingWriteSize(-slot.Block.Size());
}

TAsyncError TBlobSession::FlushBlock(int blockIndex)
{
    // TODO: verify monotonicity of blockIndex
    VERIFY_THREAD_AFFINITY(ControlThread);

    ValidateBlockIsInWindow(blockIndex);
    Ping();

    const auto& slot = GetSlot(blockIndex);
    if (slot.State == ESlotState::Empty) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::WindowError,
            "Attempt to flush an unreceived block %d (WindowStart: %d, WindowSize: %" PRISZT ")",
            blockIndex,
            WindowStartIndex_,
            Window_.size());
    }

    // IsWritten is set in the control thread, hence no need for AsyncVia.
    return slot.IsWritten.ToFuture().Apply(
        BIND(&TBlobSession::OnBlockFlushed, MakeStrong(this), blockIndex));
}

TError TBlobSession::OnBlockFlushed(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ReleaseBlocks(blockIndex);
    return Error_;
}

void TBlobSession::Cancel(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Canceling session");

    CloseLease();
    AbortWriter()
        .Apply(BIND(&TBlobSession::OnWriterAborted, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetControlInvoker()));
}

TAsyncError TBlobSession::AbortWriter()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return BIND(&TBlobSession::DoAbortWriter, MakeStrong(this))
        .AsyncVia(WriteInvoker_)
        .Run();
}

TError TBlobSession::DoAbortWriter()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!Error_.IsOK()) {
        return Error_;
    }

    LOG_DEBUG("Started aborting chunk writer");

    PROFILE_TIMING ("/blob_chunk_abort_time") {
        try {
            Writer_->Abort();
        } catch (const std::exception& ex) {
            OnIOError(TError(
                NChunkClient::EErrorCode::IOError,
                "Error aborting chunk %s",
                ~ToString(ChunkId_))
                << ex);
        }
        Writer_.Reset();
    }

    LOG_DEBUG("Finished aborting chunk writer");

    return Error_;
}

TError TBlobSession::OnWriterAborted(TError error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO(error, "Session canceled");

    ReleaseSpace();
    Failed_.Fire(error);

    return error;
}

TAsyncError TBlobSession::CloseWriter(const TChunkMeta& chunkMeta)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return BIND(&TBlobSession::DoCloseWriter,MakeStrong(this), chunkMeta)
        .AsyncVia(WriteInvoker_)
        .Run();
}

TError TBlobSession::DoCloseWriter(const TChunkMeta& chunkMeta)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!Error_.IsOK()) {
        return Error_;
    }

    LOG_DEBUG("Started closing chunk writer (ChunkSize: %" PRId64 ")",
        Writer_->GetDataSize());

    PROFILE_TIMING ("/blob_chunk_close_time") {
        try {
            auto result = Writer_->Close(chunkMeta).Get();
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        } catch (const std::exception& ex) {
            OnIOError(TError(
                NChunkClient::EErrorCode::IOError,
                "Error closing chunk %s",
                ~ToString(ChunkId_))
                << ex);
        }
    }

    LOG_DEBUG("Finished closing chunk writer");

    return Error_;
}

TErrorOr<IChunkPtr> TBlobSession::OnWriterClosed(TError error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ReleaseSpace();

    if (!error.IsOK()) {
        LOG_WARNING(error, "Session has failed to finish");
        Failed_.Fire(error);
        return error;
    }

    auto chunk = New<TStoredBlobChunk>(
        Location_,
        ChunkId_,
        Writer_->GetChunkMeta(),
        Writer_->GetChunkInfo(),
        Bootstrap_->GetMemoryUsageTracker());

    LOG_INFO("Session finished");

    Completed_.Fire(chunk);
    return IChunkPtr(chunk);
}

void TBlobSession::ReleaseBlocks(int flushedBlockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(WindowStartIndex_ <= flushedBlockIndex);

    while (WindowStartIndex_ <= flushedBlockIndex) {
        auto& slot = GetSlot(WindowStartIndex_);
        YCHECK(slot.State == ESlotState::Written);
        slot.Block = TSharedRef();
        slot.IsWritten.Reset();
        ++WindowStartIndex_;
    }

    LOG_DEBUG("Released blocks (WindowStart: %d)",
        WindowStartIndex_);
}

bool TBlobSession::IsInWindow(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return blockIndex >= WindowStartIndex_;
}

void TBlobSession::ValidateBlockIsInWindow(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!IsInWindow(blockIndex)) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::WindowError,
            "Block %d is out of the window (WindowStart: %d, WindowSize: %" PRISZT ")",
            blockIndex,
            WindowStartIndex_,
            Window_.size());
    }
}

TBlobSession::TSlot& TBlobSession::GetSlot(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(IsInWindow(blockIndex));

    while (Window_.size() <= blockIndex) {
        // NB: do not use resize here!
        // Newly added slots must get a fresh copy of IsWritten promise.
        // Using resize would cause all of these slots to share a single promise.
        Window_.push_back(TSlot());
    }

    return Window_[blockIndex];
}

TSharedRef TBlobSession::GetBlock(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ValidateBlockIsInWindow(blockIndex);

    Ping();

    const auto& slot = GetSlot(blockIndex);
    if (slot.State == ESlotState::Empty) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::WindowError,
            "Trying to retrieve a block %d that is not received yet (WindowStart: %d)",
            blockIndex,
            WindowStartIndex_);
    }

    LOG_DEBUG("Chunk block %d retrieved", blockIndex);

    return slot.Block;
}

void TBlobSession::MarkAllSlotsWritten()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // Mark all slots as written to notify all guys waiting on Flush.
    for (auto& slot : Window_) {
        if (slot.State != ESlotState::Written) {
            slot.State = ESlotState::Written;
            slot.IsWritten.Set();
        }
    }
}

void TBlobSession::ReleaseSpace()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Location_->UpdateUsedSpace(-Size_);
}

void TBlobSession::OnIOError(const TError& error)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!Error_.IsOK())
        return;

    Error_ = error;

    LOG_ERROR(Error_, "Session failed");

    Bootstrap_->GetControlInvoker()->Invoke(
        BIND(&TBlobSession::MarkAllSlotsWritten, MakeStrong(this)));

    Location_->Disable();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
