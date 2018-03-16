#include "blob_session.h"
#include "private.h"
#include "blob_chunk.h"
#include "chunk_block_manager.h"
#include "chunk_store.h"
#include "config.h"
#include "location.h"
#include "session_manager.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/ytlib/chunk_client/chunk_meta.pb.h>
#include <yt/ytlib/chunk_client/file_writer.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_connection.h>

#include <yt/core/misc/fs.h>

#include <yt/core/profiling/timing.h>

namespace NYT {
namespace NDataNode {

using namespace NProfiling;
using namespace NRpc;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NCellNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TBlobSession::DoStart()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    WriteInvoker_->Invoke(BIND(&TBlobSession::DoOpenWriter, MakeStrong(this)));

    // No need to wait for the writer to get opened.
    return VoidFuture;
}

TFuture<IChunkPtr> TBlobSession::DoFinish(
    const TChunkMeta* chunkMeta,
    const TNullable<int>& blockCount)
{
    YCHECK(chunkMeta != nullptr);
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!blockCount) {
        THROW_ERROR_EXCEPTION("Attempt to finish a blob session %v without specifying block count",
            SessionId_);
    }

    if (*blockCount != BlockCount_) {
        THROW_ERROR_EXCEPTION("Block count mismatch in blob session %v: expected %v, got %v",
            SessionId_,
            BlockCount_,
            *blockCount);
    }

    for (int blockIndex = WindowStartBlockIndex_; blockIndex < Window_.size(); ++blockIndex) {
        const auto& slot = GetSlot(blockIndex);
        if (slot.State != ESlotState::Empty) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::WindowError,
                "Attempt to finish a session with an unflushed block %v:%v",
                GetChunkId(),
                blockIndex);
        }
    }

    auto asyncResult = CloseWriter(*chunkMeta).Apply(
        BIND(&TBlobSession::OnWriterClosed, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetControlInvoker()));

    auto promise = NewPromise<IChunkPtr>();
    promise.SetFrom(asyncResult);
    promise.OnCanceled(
        BIND(&TBlobSession::OnFinishCanceled, MakeWeak(this))
            .Via(Bootstrap_->GetControlInvoker()));

    return promise.ToFuture();
}

TChunkInfo TBlobSession::GetChunkInfo() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Writer_->GetChunkInfo();
}

TFuture<void> TBlobSession::DoPutBlocks(
    int startBlockIndex,
    const std::vector<TBlock>& blocks,
    bool enableCaching)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (blocks.empty()) {
        return VoidFuture;
    }

    // Make all acquisitions in advance to ensure that this error is retriable.
    auto* tracker = Bootstrap_->GetMemoryUsageTracker();
    std::vector<TNodeMemoryTrackerGuard> memoryTrackerGuards;
    for (const auto& block : blocks) {
        auto guardOrError = TNodeMemoryTrackerGuard::TryAcquire(
            tracker,
            EMemoryCategory::BlobSession,
            block.Size());
        if (!guardOrError.IsOK()) {
            return MakeFuture(TError(guardOrError).SetCode(NChunkClient::EErrorCode::WriteThrottlingActive));
        }
        memoryTrackerGuards.emplace_back(std::move(guardOrError.Value()));
    }

    auto chunkBlockManager = Bootstrap_->GetChunkBlockManager();

    std::vector<int> receivedBlockIndexes;
    for (int localIndex = 0; localIndex < static_cast<int>(blocks.size()); ++localIndex) {
        int blockIndex = startBlockIndex + localIndex;
        const auto& block = blocks[localIndex];
        TBlockId blockId(GetChunkId(), blockIndex);
        ValidateBlockIsInWindow(blockIndex);

        if (!Location_->HasEnoughSpace(block.Size())) {
            return MakeFuture(TError(
                NChunkClient::EErrorCode::NoLocationAvailable,
                "No enough space left on location"));
        }

        auto& slot = GetSlot(blockIndex);
        if (slot.State != ESlotState::Empty) {
            if (TRef::AreBitwiseEqual(slot.Block.Data, block.Data)) {
                LOG_WARNING("Skipped duplicate block (Block: %v)", blockIndex);
                continue;
            }

            return MakeFuture(TError(
                NChunkClient::EErrorCode::BlockContentMismatch,
                "Block %v:%v with a different content already received",
                GetChunkId(),
                blockIndex)
                << TErrorAttribute("window_start", WindowStartBlockIndex_));
        }

        ++BlockCount_;

        slot.State = ESlotState::Received;
        slot.Block = block;
        slot.MemoryTrackerGuard = std::move(memoryTrackerGuards[localIndex]);

        if (enableCaching) {
            chunkBlockManager->PutCachedBlock(blockId, block, Null);
        }

        Location_->UpdateUsedSpace(block.Size());
        receivedBlockIndexes.push_back(blockIndex);
    }

    auto totalSize = GetByteSize(blocks);
    Size_ += totalSize;

    LOG_DEBUG_UNLESS(receivedBlockIndexes.empty(), "Blocks received (Blocks: %v, TotalSize: %v)",
        receivedBlockIndexes,
        totalSize);

    // Organize blocks in packs of BytesPerWrite size and pass them to the WriterThread.
    int beginBlockIndex = WindowIndex_;
    i64 totalBlocksSize = 0;
    std::vector<TBlock> blocksToWrite;

    auto enqueueBlocks = [&] () {
        YCHECK(blocksToWrite.size() == WindowIndex_ - beginBlockIndex);
        if (beginBlockIndex == WindowIndex_) {
            return;
        }

        BIND(
            &TBlobSession::DoWriteBlocks,
            MakeStrong(this),
            std::move(blocksToWrite),
            beginBlockIndex,
            WindowIndex_)
        .AsyncVia(WriteInvoker_)
        .Run()
        .Subscribe(
            BIND(&TBlobSession::OnBlocksWritten, MakeStrong(this), beginBlockIndex, WindowIndex_)
                .Via(Bootstrap_->GetControlInvoker()));

        beginBlockIndex = WindowIndex_;
        totalBlocksSize = 0;
        blocksToWrite.clear();
    };

    while (true) {
        if (WindowIndex_ >= Window_.size()) {
            enqueueBlocks();
            break;
        }

        auto& slot = GetSlot(WindowIndex_);
        YCHECK(slot.State == ESlotState::Received || slot.State == ESlotState::Empty);
        if (slot.State == ESlotState::Empty) {
            enqueueBlocks();
            break;
        }

        slot.PendingIOGuard = Location_->IncreasePendingIOSize(
            EIODirection::Write,
            Options_.WorkloadDescriptor,
            slot.Block.Size());

        blocksToWrite.emplace_back(slot.Block);
        totalBlocksSize += slot.Block.Size();

        ++WindowIndex_;

        if (totalBlocksSize >= Config_->BytesPerWrite) {
            enqueueBlocks();
        }
    }

    auto netThrottler = Bootstrap_->GetInThrottler(Options_.WorkloadDescriptor);
    auto diskThrottler = Location_->GetInThrottler(Options_.WorkloadDescriptor);
    return Combine(std::vector<TFuture<void>>({
        netThrottler->Throttle(totalSize),
        diskThrottler->Throttle(totalSize) }));
}

TFuture<void> TBlobSession::DoSendBlocks(
    int firstBlockIndex,
    int blockCount,
    const TNodeDescriptor& targetDescriptor)
{
    const auto& channelFactory = Bootstrap_
        ->GetMasterClient()
        ->GetNativeConnection()
        ->GetChannelFactory();
    auto channel = channelFactory->CreateChannel(targetDescriptor.GetAddressOrThrow(Bootstrap_->GetLocalNetworks()));
    TDataNodeServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Config_->NodeRpcTimeout);

    auto req = proxy.PutBlocks();
    req->SetMultiplexingBand(EMultiplexingBand::Heavy);
    ToProto(req->mutable_session_id(), SessionId_);
    req->set_first_block_index(firstBlockIndex);

    i64 requestSize = 0;

    std::vector<TBlock> blocks;
    for (int blockIndex = firstBlockIndex; blockIndex < firstBlockIndex + blockCount; ++blockIndex) {
        auto block = GetBlock(blockIndex);

        blocks.push_back(std::move(block));
        requestSize += block.Size();
    }
    SetRpcAttachedBlocks(req, blocks);

    auto throttler = Bootstrap_->GetOutThrottler(Options_.WorkloadDescriptor);
    return throttler->Throttle(requestSize).Apply(BIND([=] () {
        return req->Invoke().As<void>();
    }));
}

void TBlobSession::DoWriteBlocks(const std::vector<TBlock>& blocks, int beginBlockIndex, int endBlockIndex)
{
    // Thread affinity: WriterThread

    THROW_ERROR_EXCEPTION_IF_FAILED(Error_);

    for (int index = 0; index <  endBlockIndex - beginBlockIndex; ++index) {
        if (Canceled_.load()) {
            return;
        }

        const auto& block = blocks[index];
        int blockIndex = beginBlockIndex + index;

        LOG_DEBUG("Started writing block (BlockIndex: %v, BlockSize: %v)",
            blockIndex,
            block.Size());

        TWallTimer timer;
        TBlockId blockId(GetChunkId(), blockIndex);
        try {
            if (!Writer_->WriteBlock(block)) {
                auto result = Writer_->GetReadyEvent().Get();
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
                Y_UNREACHABLE();
            }
        } catch (const TBlockChecksumValidationException& ex) {
            SetFailed(TError(
                NChunkClient::EErrorCode::InvalidBlockChecksum,
                "Invalid checksum detected in chunk block %v",
                blockId)
                << TErrorAttribute("expected_checksum", ex.GetExpected())
                << TErrorAttribute("actual_checksum", ex.GetActual()),
                /* fatal */ false);
        } catch (const std::exception& ex) {
            SetFailed(TError(
                NChunkClient::EErrorCode::IOError,
                "Error writing chunk block %v",
                blockId)
                << ex);
        }

        auto writeTime = timer.GetElapsedTime();

        LOG_DEBUG("Finished writing block (BlockIndex: %v)", blockIndex);

        auto& locationProfiler = Location_->GetProfiler();
        locationProfiler.Enqueue("/blob_block_write_size", block.Size(), EMetricType::Gauge);
        locationProfiler.Enqueue("/blob_block_write_time", writeTime.MicroSeconds(), EMetricType::Gauge);
        locationProfiler.Enqueue("/blob_block_write_throughput", block.Size() * 1000000 / (1 + writeTime.MicroSeconds()), EMetricType::Gauge);

        Location_->IncreaseCompletedIOSize(EIODirection::Write, Options_.WorkloadDescriptor, block.Size());

        THROW_ERROR_EXCEPTION_IF_FAILED(Error_);
    }
}

void TBlobSession::OnBlocksWritten(int beginBlockIndex, int endBlockIndex, const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (Canceled_.load()) {
        return;
    }

    for (int blockIndex = beginBlockIndex; blockIndex < endBlockIndex; ++blockIndex) {
        auto& slot = GetSlot(blockIndex);
        slot.PendingIOGuard.Release();
        if (error.IsOK()) {
            YCHECK(slot.State == ESlotState::Received);
            slot.State = ESlotState::Written;
            slot.WrittenPromise.Set(TError());
        }
    }
}

TFuture<void> TBlobSession::DoFlushBlocks(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!IsInWindow(blockIndex)) {
        LOG_DEBUG("Blocks are already flushed (BlockIndex: %v)", blockIndex);
        return VoidFuture;
    }

    const auto& slot = GetSlot(blockIndex);
    if (slot.State == ESlotState::Empty) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::WindowError,
            "Attempt to flush an unreceived block %v:%v",
            GetChunkId(),
            blockIndex);
    }

    // WrittenPromise is set in the control thread, hence no need for AsyncVia.
    return slot.WrittenPromise.ToFuture().Apply(
        BIND(&TBlobSession::OnBlockFlushed, MakeStrong(this), blockIndex));
}

void TBlobSession::OnBlockFlushed(int blockIndex, const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (Canceled_.load()) {
        return;
    }

    ReleaseBlocks(blockIndex);

    THROW_ERROR_EXCEPTION_IF_FAILED(error);
}

void TBlobSession::DoCancel(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    for (auto& slot : Window_) {
        auto& promise = slot.WrittenPromise;
        if (promise) {
            promise.TrySet(error);
        }
    }

    AbortWriter()
        .Apply(BIND(&TBlobSession::OnWriterAborted, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetControlInvoker()));
}

void TBlobSession::DoOpenWriter()
{
    // Thread affinity: WriterThread

    LOG_DEBUG("Started opening blob chunk writer");

    PROFILE_TIMING ("/blob_chunk_open_time") {
        try {
            auto fileName = Location_->GetChunkPath(GetChunkId());
            Writer_ = New<TFileWriter>(Location_->GetIOEngine(), GetChunkId(), fileName, Options_.SyncOnClose, Options_.EnableWriteDirectIO);
            // File writer opens synchronously.
            Writer_->Open()
                .Get()
                .ThrowOnError();
        }
        catch (const std::exception& ex) {
            SetFailed(TError(
                NChunkClient::EErrorCode::IOError,
                "Error creating chunk %v",
                SessionId_)
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
    // Thread affinity: WriterThread

    THROW_ERROR_EXCEPTION_IF_FAILED(Error_);

    LOG_DEBUG("Started aborting chunk writer");

    PROFILE_TIMING ("/blob_chunk_abort_time") {
        try {
            Writer_->Abort();
        } catch (const std::exception& ex) {
            SetFailed(TError(
                NChunkClient::EErrorCode::IOError,
                "Error aborting chunk %v",
                SessionId_)
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
    // Thread affinity: WriterThread

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
                SessionId_)
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
    descriptor.Id = GetChunkId();
    descriptor.DiskSpace = Writer_->GetChunkInfo().disk_space();
    auto chunk = New<TStoredBlobChunk>(
        Bootstrap_,
        Location_,
        descriptor,
        &Writer_->GetChunkMeta());

    const auto& chunkStore = Bootstrap_->GetChunkStore();
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
        slot.Block = TBlock();
        slot.MemoryTrackerGuard.Release();
        slot.PendingIOGuard.Release();
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
            GetChunkId(),
            blockIndex);
    }
}

TBlobSession::TSlot& TBlobSession::GetSlot(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(IsInWindow(blockIndex));

    while (Window_.size() <= blockIndex) {
        Window_.emplace_back();
        auto& slot = Window_.back();
        slot.WrittenPromise.OnCanceled(
            BIND(
                &TBlobSession::OnSlotCanceled,
                MakeWeak(this),
                WindowStartBlockIndex_ + static_cast<int>(Window_.size()) - 1)
            .Via(Bootstrap_->GetControlInvoker()));
    }

    return Window_[blockIndex];
}

TBlock TBlobSession::GetBlock(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ValidateBlockIsInWindow(blockIndex);

    Ping();

    const auto& slot = GetSlot(blockIndex);
    if (slot.State == ESlotState::Empty) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::WindowError,
            "Trying to retrieve a block %v:%v that is not received yet",
            GetChunkId(),
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

void TBlobSession::SetFailed(const TError& error, bool fatal)
{
    // Thread affinity: WriterThread

    if (!Error_.IsOK()) {
        return;
    }

    Error_ = TError("Session failed") << error;

    Bootstrap_->GetControlInvoker()->Invoke(
        BIND(&TBlobSession::MarkAllSlotsWritten, MakeStrong(this), error));

    if (fatal) {
        Location_->Disable(Error_);
        Y_UNREACHABLE(); // Disable() exits the process.
    }
}

void TBlobSession::OnSlotCanceled(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Cancel(TError("Session canceled at block %v:%v",
        GetChunkId(),
        blockIndex));
}

void TBlobSession::OnFinishCanceled()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Cancel(TError("Session canceled during finish"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
