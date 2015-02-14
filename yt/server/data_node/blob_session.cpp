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

static NProfiling::TRateCounter DiskWriteThroughputCounter("/disk_write_throughput_counter");

////////////////////////////////////////////////////////////////////////////////

TBlobSession::TBlobSession(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap,
    const TChunkId& chunkId,
    const TSessionOptions& options,
    TLocationPtr location,
    TLease lease)
    : TSessionBase(
        config,
        bootstrap,
        chunkId,
        options,
        location,
        lease)
{ }

TFuture<void> TBlobSession::DoStart()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    WriteInvoker_->Invoke(BIND(&TBlobSession::DoOpenWriter, MakeStrong(this)));

    // No need to wait for the writer to get opened.
    return VoidFuture;
}

TFuture<IChunkPtr> TBlobSession::DoFinish(
    const TChunkMeta& chunkMeta,
    const TNullable<int>& blockCount)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!blockCount) {
        THROW_ERROR_EXCEPTION("Attempt to finish a blob session %v without specifying block count",
            ChunkId_);
    }

    if (*blockCount != BlockCount_) {
        THROW_ERROR_EXCEPTION("Block count mismatch in blob session %v: expected %v, got %v",
            ChunkId_,
            BlockCount_,
            *blockCount);
    }

    for (int blockIndex = WindowStartBlockIndex_; blockIndex < Window_.size(); ++blockIndex) {
        const auto& slot = GetSlot(blockIndex);
        if (slot.State != ESlotState::Empty) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::WindowError,
                "Attempt to finish a session with an unflushed block %v:%v",
                ChunkId_,
                blockIndex);
        }
    }

    return CloseWriter(chunkMeta).Apply(
        BIND(&TBlobSession::OnWriterClosed, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetControlInvoker()));
}

TChunkInfo TBlobSession::GetChunkInfo() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Writer_->GetChunkInfo();
}

TFuture<void> TBlobSession::DoPutBlocks(
    int startBlockIndex,
    const std::vector<TSharedRef>& blocks,
    bool enableCaching)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (blocks.empty()) {
        return VoidFuture;
    }

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
                LOG_WARNING("Skipped duplicate block (Block: %v)", blockIndex);
                continue;
            }

            return MakeFuture(TError(
                NChunkClient::EErrorCode::BlockContentMismatch,
                "Block %v:%v with a different content already received",
                ChunkId_,
                blockIndex)
                << TErrorAttribute("window_start", WindowStartBlockIndex_));
        }

        ++BlockCount_;

        slot.State = ESlotState::Received;
        slot.Block = block;

        if (enableCaching) {
            blockStore->PutBlock(blockId, block, Null);
        }

        Location_->UpdateUsedSpace(block.Size());
        Size_ += block.Size();
        requestSize += block.Size();

        LOG_DEBUG("Block received (Block: %v)", blockIndex);

        ++blockIndex;
    }

    auto sessionManager = Bootstrap_->GetSessionManager();
    while (WindowIndex_ < Window_.size()) {
        const auto& slot = GetSlot(WindowIndex_);
        YCHECK(slot.State == ESlotState::Received || slot.State == ESlotState::Empty);
        if (slot.State == ESlotState::Empty)
            break;

        sessionManager->UpdatePendingWriteSize(slot.Block.Size());

        BIND(
            &TBlobSession::DoWriteBlock,
            MakeStrong(this),
            slot.Block,
            WindowIndex_)
        .AsyncVia(WriteInvoker_)
        .Run()
        .Subscribe(
            BIND(&TBlobSession::OnBlockWritten, MakeStrong(this), WindowIndex_)
                .Via(Bootstrap_->GetControlInvoker()));
        ++WindowIndex_;
    }

    auto throttler = Bootstrap_->GetInThrottler(Options_.SessionType);
    return throttler->Throttle(requestSize);
}

TFuture<void> TBlobSession::DoSendBlocks(
    int firstBlockIndex,
    int blockCount,
    const TNodeDescriptor& target)
{
    TDataNodeServiceProxy proxy(ChannelFactory->CreateChannel(target.GetInterconnectAddress()));
    proxy.SetDefaultTimeout(Config_->NodeRpcTimeout);

    auto req = proxy.PutBlocks();
    ToProto(req->mutable_chunk_id(), ChunkId_);
    req->set_first_block_index(firstBlockIndex);

    i64 requestSize = 0;
    for (int blockIndex = firstBlockIndex; blockIndex < firstBlockIndex + blockCount; ++blockIndex) {
        auto block = GetBlock(blockIndex);
        req->Attachments().push_back(block);
        requestSize += block.Size();
    }

    auto throttler = Bootstrap_->GetOutThrottler(Options_.SessionType);
    return throttler->Throttle(requestSize).Apply(BIND([=] () {
        return req->Invoke().As<void>();
    }));
}

void TBlobSession::DoWriteBlock(const TSharedRef& block, int blockIndex)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    THROW_ERROR_EXCEPTION_IF_FAILED(Error_);

    LOG_DEBUG("Started writing block %v (BlockSize: %v)",
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
        SetFailed(TError(
            NChunkClient::EErrorCode::IOError,
            "Error writing chunk block %v",
            blockId)
            << ex);
    }

    auto writeTime = timer.GetElapsed();

    LOG_DEBUG("Finished writing block %v", blockIndex);

    auto& locationProfiler = Location_->Profiler();
    locationProfiler.Enqueue("/blob_block_write_size", block.Size());
    locationProfiler.Enqueue("/blob_block_write_time", writeTime.MicroSeconds());
    locationProfiler.Enqueue("/blob_block_write_speed", block.Size() * 1000000 / (1 + writeTime.MicroSeconds()));

    DataNodeProfiler.Increment(DiskWriteThroughputCounter, block.Size());

    THROW_ERROR_EXCEPTION_IF_FAILED(Error_);
}

void TBlobSession::OnBlockWritten(int blockIndex, const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto& slot = GetSlot(blockIndex);

    // NB: Update pending write size before setting the promise as the subscriber
    // is likely to erase the slot.
    auto sessionManager = Bootstrap_->GetSessionManager();
    sessionManager->UpdatePendingWriteSize(-slot.Block.Size());

    if (error.IsOK()) {
        YCHECK(slot.State == ESlotState::Received);
        slot.State = ESlotState::Written;
        slot.WrittenPromise.Set(TError());
    }
}

TFuture<void> TBlobSession::DoFlushBlocks(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // TODO(psushin): verify monotonicity of blockIndex
    ValidateBlockIsInWindow(blockIndex);

    const auto& slot = GetSlot(blockIndex);
    if (slot.State == ESlotState::Empty) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::WindowError,
            "Attempt to flush an unreceived block %v:%v",
            ChunkId_,
            blockIndex);
    }

    // WrittenPromise is set in the control thread, hence no need for AsyncVia.
    return slot.WrittenPromise.ToFuture().Apply(
        BIND(&TBlobSession::OnBlockFlushed, MakeStrong(this), blockIndex));
}

void TBlobSession::OnBlockFlushed(int blockIndex, const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ReleaseBlocks(blockIndex);

    THROW_ERROR_EXCEPTION_IF_FAILED(error);
}

void TBlobSession::DoCancel()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    AbortWriter()
        .Apply(BIND(&TBlobSession::OnWriterAborted, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetControlInvoker()));
}

void TBlobSession::DoOpenWriter()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Started opening blob chunk writer");

    PROFILE_TIMING ("/blob_chunk_open_time") {
        try {
            auto fileName = Location_->GetChunkPath(ChunkId_);
            Writer_ = New<TFileWriter>(fileName, Options_.SyncOnClose);
            auto result = Writer_->Open();

            // File writer opens synchronously.
            YCHECK(result.IsSet());
            YCHECK(result.Get().IsOK());
        }
        catch (const std::exception& ex) {
            SetFailed(TError(
                NChunkClient::EErrorCode::IOError,
                "Error creating chunk %v",
                ChunkId_)
                << ex);
            return;
        }
    }

    LOG_DEBUG("Finished opening blob chunk writer");
}

TFuture<void> TBlobSession::AbortWriter()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return BIND(&TBlobSession::DoAbortWriter, MakeStrong(this))
        .AsyncVia(WriteInvoker_)
        .Run();
}

void TBlobSession::DoAbortWriter()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    THROW_ERROR_EXCEPTION_IF_FAILED(Error_);

    LOG_DEBUG("Started aborting chunk writer");

    PROFILE_TIMING ("/blob_chunk_abort_time") {
        try {
            Writer_->Abort();
        } catch (const std::exception& ex) {
            SetFailed(TError(
                NChunkClient::EErrorCode::IOError,
                "Error aborting chunk %v",
                ChunkId_)
                << ex);
        }
        Writer_.Reset();
    }

    LOG_DEBUG("Finished aborting chunk writer");

    THROW_ERROR_EXCEPTION_IF_FAILED(Error_);
}

void TBlobSession::OnWriterAborted(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO(error, "Session canceled");

    ReleaseSpace();
    Finished_.Fire(error);

    THROW_ERROR_EXCEPTION_IF_FAILED(error);
}

TFuture<void> TBlobSession::CloseWriter(const TChunkMeta& chunkMeta)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return BIND(&TBlobSession::DoCloseWriter,MakeStrong(this), chunkMeta)
        .AsyncVia(WriteInvoker_)
        .Run();
}

void TBlobSession::DoCloseWriter(const TChunkMeta& chunkMeta)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    THROW_ERROR_EXCEPTION_IF_FAILED(Error_);

    LOG_DEBUG("Started closing chunk writer (ChunkSize: %v)",
        Writer_->GetDataSize());

    PROFILE_TIMING ("/blob_chunk_close_time") {
        try {
            auto result = Writer_->Close(chunkMeta).Get();
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        } catch (const std::exception& ex) {
            SetFailed(TError(
                NChunkClient::EErrorCode::IOError,
                "Error closing chunk %v",
                ChunkId_)
                << ex);
        }
    }

    LOG_DEBUG("Finished closing chunk writer");

    THROW_ERROR_EXCEPTION_IF_FAILED(Error_);
}

IChunkPtr TBlobSession::OnWriterClosed(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ReleaseSpace();

    if (!error.IsOK()) {
        LOG_WARNING(error, "Session has failed to finish");
        Finished_.Fire(error);
        THROW_ERROR(error);
    }

    TChunkDescriptor descriptor;
    descriptor.Id = ChunkId_;
    descriptor.DiskSpace = Writer_->GetChunkInfo().disk_space();
    auto chunk = New<TStoredBlobChunk>(
        Bootstrap_,
        Location_,
        descriptor,
        &Writer_->GetChunkMeta());

    auto chunkStore = Bootstrap_->GetChunkStore();
    chunkStore->RegisterNewChunk(chunk);

    Finished_.Fire(TError());

    return chunk;
}

void TBlobSession::ReleaseBlocks(int flushedBlockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(WindowStartBlockIndex_ <= flushedBlockIndex);

    while (WindowStartBlockIndex_ <= flushedBlockIndex) {
        auto& slot = GetSlot(WindowStartBlockIndex_);
        YCHECK(slot.State == ESlotState::Written);
        slot.Block = TSharedRef();
        slot.WrittenPromise.Reset();
        ++WindowStartBlockIndex_;
    }

    LOG_DEBUG("Released blocks (WindowStart: %v)",
        WindowStartBlockIndex_);
}

bool TBlobSession::IsInWindow(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return blockIndex >= WindowStartBlockIndex_;
}

void TBlobSession::ValidateBlockIsInWindow(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!IsInWindow(blockIndex)) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::WindowError,
            "Block %v:%v is out of the window",
            ChunkId_,
            blockIndex);
    }
}

TBlobSession::TSlot& TBlobSession::GetSlot(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(IsInWindow(blockIndex));

    while (Window_.size() <= blockIndex) {
        // NB: do not use resize here!
        // Newly added slots must get a fresh copy of WrittenPromise promise.
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
            "Trying to retrieve a block %v:%v that is not received yet",
            ChunkId_,
            blockIndex);
    }

    LOG_DEBUG("Block retrieved (Block: %v)", blockIndex);

    return slot.Block;
}

void TBlobSession::MarkAllSlotsWritten(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    for (auto& slot : Window_) {
        if (slot.State == ESlotState::Received) {
            slot.State = ESlotState::Written;
            slot.WrittenPromise.Set(error);
        }
    }
}

void TBlobSession::ReleaseSpace()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Location_->UpdateUsedSpace(-Size_);
}

void TBlobSession::SetFailed(const TError& error)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!Error_.IsOK())
        return;

    Error_ = TError("Session failed") << error;

    Bootstrap_->GetControlInvoker()->Invoke(
        BIND(&TBlobSession::MarkAllSlotsWritten, MakeStrong(this), error));

    Location_->Disable(Error_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
