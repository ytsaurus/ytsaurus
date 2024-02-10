#include "blob_session.h"
#include "bootstrap.h"
#include "private.h"
#include "blob_chunk.h"
#include "chunk_store.h"
#include "config.h"
#include "location.h"
#include "session_manager.h"

#include <yt/yt/server/lib/io/chunk_file_reader.h>
#include <yt/yt/server/lib/io/chunk_file_writer.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/ring_queue.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NDataNode {

using namespace NProfiling;
using namespace NRpc;
using namespace NIO;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NClusterNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsOutOfDiskSpaceError(const TError& error)
{
    auto ioError = error.FindMatching(NFS::EErrorCode::IOError);
    if (!ioError) {
        return false;
    }
    return ioError->Attributes().Get<int>("status") == ENOSPC;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TBlobWritePipeline
    : public TRefCounted
{
public:
    TBlobWritePipeline(
        TChunkLocationPtr location,
        IInvokerPtr sessionInvoker,
        NChunkClient::TChunkId chunkId,
        TSessionOptions options,
        NLogging::TLogger logger)
        : Location_(std::move(location))
        , SessionInvoker_(std::move(sessionInvoker))
        , Options_(options)
        , Logger(std::move(logger))
        , Writer_(New<TChunkFileWriter>(
            Location_->GetIOEngine(),
            chunkId,
            Location_->GetChunkPath(chunkId),
            Options_.SyncOnClose))
    { }


    TFuture<void> Open()
    {
        return EnqueueCommand(
            BIND(&TBlobWritePipeline::DoOpen, MakeStrong(this)));
    }

    TFuture<void> WriteBlocks(int firstBlockIndex, std::vector<TBlock> blocks)
    {
        return EnqueueCommand(
            BIND(&TBlobWritePipeline::DoWriteBlocks, MakeStrong(this), firstBlockIndex, std::move(blocks)));
    }

    TFuture<void> Close(TRefCountedChunkMetaPtr chunkMeta)
    {
        return EnqueueCommand(
            BIND(&TBlobWritePipeline::DoClose, MakeStrong(this), std::move(chunkMeta)));
    }

    TFuture<void> Abort()
    {
        return EnqueueCommand(
            BIND(&TBlobWritePipeline::DoAbort, MakeStrong(this)));
    }


    const TChunkInfo& GetChunkInfo() const
    {
        return Writer_->GetChunkInfo();
    }

    const TRefCountedChunkMetaPtr& GetChunkMeta() const
    {
        return Writer_->GetChunkMeta();
    }

private:
    const TChunkLocationPtr Location_;
    const IInvokerPtr SessionInvoker_;
    const TSessionOptions Options_;
    const NLogging::TLogger Logger;

    const NIO::TChunkFileWriterPtr Writer_;

    using TCommand = TCallback<TFuture<void>()>;

    struct TCommandEntry
    {
        TCommand Command;
        TPromise<void> Promise;
    };

    TRingQueue<TCommandEntry> CommandQueue_;

    bool CommandRunning_ = false;

    TFuture<void> EnqueueCommand(TCommand command)
    {
        VERIFY_INVOKER_AFFINITY(SessionInvoker_);

        auto promise = NewPromise<void>();
        CommandQueue_.push(TCommandEntry{
            .Command = std::move(command),
            .Promise = promise
        });
        RunCommands();
        return promise.ToFuture();
    }

    void RunCommands()
    {
        VERIFY_INVOKER_AFFINITY(SessionInvoker_);

        if (CommandRunning_) {
            return;
        }

        while (!CommandQueue_.empty()) {
            auto entry = std::move(CommandQueue_.front());
            CommandQueue_.pop();

            if (entry.Promise.IsCanceled()) {
                entry.Promise.Set(TError(NYT::EErrorCode::Canceled, "Pipeline command canceled"));
            } else {
                YT_VERIFY(!CommandRunning_);
                CommandRunning_ = true;
                entry.Command().Subscribe(
                    BIND(&TBlobWritePipeline::OnCommandFinished, MakeStrong(this), entry.Promise)
                        .Via(SessionInvoker_));
                break;
            }
        }
    }

    void OnCommandFinished(const TPromise<void>& promise, const TError& error)
    {
        VERIFY_INVOKER_AFFINITY(SessionInvoker_);

        YT_VERIFY(CommandRunning_);
        CommandRunning_ = false;
        promise.Set(error);

        RunCommands();
    }


    TFuture<void> DoOpen()
    {
        VERIFY_INVOKER_AFFINITY(SessionInvoker_);

        YT_LOG_DEBUG("Started opening blob chunk writer");

        TWallTimer timer;

        return Writer_->Open().Apply(
            BIND([=, this, this_ = MakeStrong(this)] {
                auto time = timer.GetElapsedTime();

                YT_LOG_DEBUG("Finished opening blob chunk writer (Time: %v)",
                    time);

                auto& performanceCounters = Location_->GetPerformanceCounters();
                performanceCounters.BlobChunkWriterOpenTime.Record(time);
            }));
    }

    TFuture<void> DoWriteBlocks(int firstBlockIndex, const std::vector<TBlock>& blocks)
    {
        VERIFY_INVOKER_AFFINITY(SessionInvoker_);

        auto blockCount = static_cast<int>(blocks.size());
        auto totalSize = GetByteSize(blocks);

        YT_LOG_DEBUG("Started writing blocks (BlockIndexes: %v-%v, TotalSize: %v)",
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
            totalSize);

        TWallTimer timer;

        // This is how TFileWriter works.
        YT_VERIFY(!Writer_->WriteBlocks(Options_.WorkloadDescriptor, blocks));

        return Writer_->GetReadyEvent().Apply(
            BIND([=, this, this_ = MakeStrong(this)] {
                auto time = timer.GetElapsedTime();

                YT_LOG_DEBUG("Finished writing blocks (BlockIndexes: %v-%v, Time: %v)",
                    firstBlockIndex,
                    firstBlockIndex + blockCount - 1,
                    time);

                auto& performanceCounters = Location_->GetPerformanceCounters();
                performanceCounters.BlobBlockWriteSize.Record(totalSize);
                performanceCounters.BlobBlockWriteTime.Record(time);
                performanceCounters.BlobBlockWriteBytes.Increment(totalSize);

                Location_->IncreaseCompletedIOSize(EIODirection::Write, Options_.WorkloadDescriptor, totalSize);
            }));
    }

    TFuture<void> DoClose(const TRefCountedChunkMetaPtr& chunkMeta)
    {
        VERIFY_INVOKER_AFFINITY(SessionInvoker_);

        YT_LOG_DEBUG("Started closing chunk writer (ChunkSize: %v)",
            Writer_->GetDataSize());

        auto deferredChunkMeta = New<TDeferredChunkMeta>();
        deferredChunkMeta->MergeFrom(*chunkMeta);

        TWallTimer timer;

        return Writer_->Close(Options_.WorkloadDescriptor, deferredChunkMeta).Apply(
            BIND([=, this, this_ = MakeStrong(this)] {
                auto time = timer.GetElapsedTime();

                YT_LOG_DEBUG("Finished closing chunk writer (Time: %v)",
                    time);

                auto& performanceCounters = Location_->GetPerformanceCounters();
                performanceCounters.BlobChunkWriterCloseTime.Record(time);
            }));
    }

    TFuture<void> DoAbort()
    {
        VERIFY_INVOKER_AFFINITY(SessionInvoker_);

        YT_LOG_DEBUG("Started aborting chunk writer");

        TWallTimer timer;

        return Writer_->Cancel().Apply(
            BIND([=, this, this_ = MakeStrong(this)] {
                auto time = timer.GetElapsedTime();

                YT_LOG_DEBUG("Finished aborting chunk writer (Time: %v)",
                    time);

                auto& performanceCounters = Location_->GetPerformanceCounters();
                performanceCounters.BlobChunkWriterAbortTime.Record(time);
            }));
    }
};

DEFINE_REFCOUNTED_TYPE(TBlobWritePipeline)

////////////////////////////////////////////////////////////////////////////////

TBlobSession::TBlobSession(
    TDataNodeConfigPtr config,
    IBootstrap* bootstrap,
    TSessionId sessionId,
    const TSessionOptions& options,
    TStoreLocationPtr location,
    NConcurrency::TLease lease,
    TLockedChunkGuard lockedChunkGuard)
    : TSessionBase(
        std::move(config),
        bootstrap,
        sessionId,
        options,
        std::move(location),
        std::move(lease),
        std::move(lockedChunkGuard))
    , Pipeline_(New<TBlobWritePipeline>(
        Location_,
        SessionInvoker_,
        SessionId_.ChunkId,
        Options_,
        Logger))
{ }

TFuture<void> TBlobSession::DoStart()
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    Pipeline_->Open()
        .Subscribe(BIND(&TBlobSession::OnStarted, MakeStrong(this))
            .Via(SessionInvoker_));

    // No need to wait for the writer to get opened.
    return VoidFuture;
}

void TBlobSession::OnStarted(const TError& error)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    if (!error.IsOK()) {
        if (IsOutOfDiskSpaceError(error)) {
            SetFailed(
                TError(
                    NChunkClient::EErrorCode::NoSpaceLeftOnDevice,
                    "Not enough space to start blob session for chunk %v",
                    GetChunkId())
                << error,
                /*fatal*/ false);
        } else {
            SetFailed(
                TError(
                    NChunkClient::EErrorCode::IOError,
                    "Error starting blob session for chunk %v",
                    GetChunkId())
                << error,
                /*fatal*/ true);
        }
    }
}

TFuture<TChunkInfo> TBlobSession::DoFinish(
    const TRefCountedChunkMetaPtr& chunkMeta,
    std::optional<int> blockCount)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);
    YT_VERIFY(chunkMeta);

    if (!blockCount) {
        return MakeFuture<TChunkInfo>(TError("Attempt to finish a blob session %v without specifying block count",
            SessionId_));
    }

    if (*blockCount != BlockCount_) {
        return MakeFuture<TChunkInfo>(TError("Block count mismatch in blob session %v: expected %v, got %v",
            SessionId_,
            BlockCount_,
            *blockCount));
    }

    for (int blockIndex = WindowStartBlockIndex_; blockIndex < std::ssize(Window_); ++blockIndex) {
        const auto& slot = GetSlot(blockIndex);
        if (slot.State != ESlotState::Empty) {
            return MakeFuture<TChunkInfo>(TError(
                NChunkClient::EErrorCode::WindowError,
                "Attempt to finish a session with an unflushed block %v",
                TBlockId(GetChunkId(), blockIndex)));
        }
    }

    if (!Error_.IsOK()) {
        return MakeFuture<TChunkInfo>(Error_);
    }

    return Pipeline_->Close(chunkMeta)
        .Apply(BIND(&TBlobSession::OnFinished, MakeStrong(this))
            .AsyncVia(SessionInvoker_));
}

TChunkInfo TBlobSession::OnFinished(const TError& error)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    if (!error.IsOK()) {
        if (IsOutOfDiskSpaceError(error)) {
            SetFailed(
                TError(
                    NChunkClient::EErrorCode::NoSpaceLeftOnDevice,
                    "Not enough space to write blocks of chunk %v",
                    GetChunkId())
                << error,
                /*fatal*/ false);
        } else {
            SetFailed(
                TError(
                    NChunkClient::EErrorCode::IOError,
                    "Error writing blocks of chunk %v",
                    SessionId_)
                << error,
                /*fatal*/ true);
        }
    }

    ReleaseSpace();

    if (Error_.IsOK()) {
        try {
            auto descriptor = TChunkDescriptor(
                GetChunkId(),
                Pipeline_->GetChunkInfo().disk_space());

            auto chunk = New<TStoredBlobChunk>(
                TChunkContext::Create(Bootstrap_),
                Location_,
                descriptor,
                Pipeline_->GetChunkMeta());

            const auto& chunkStore = Bootstrap_->GetChunkStore();
            chunkStore->RegisterNewChunk(chunk, /*session*/ this, std::move(LockedChunkGuard_));
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Failed to finish session");
            Error_ = TError(ex);
        }
    }

    Finished_.Fire(Error_);

    Error_.ThrowOnError();

    return Pipeline_->GetChunkInfo();
}

TFuture<NIO::TIOCounters> TBlobSession::DoPutBlocks(
    int startBlockIndex,
    const std::vector<TBlock>& blocks,
    bool enableCaching)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    if (blocks.empty()) {
        return MakeFuture<NIO::TIOCounters>({});
    }

    // Make all acquisitions in advance to ensure that this error is retriable.
    const auto& memoryTracker = Location_->GetWriteMemoryTracker();
    std::vector<TMemoryUsageTrackerGuard> memoryTrackerGuards;
    memoryTrackerGuards.reserve(blocks.size());
    for (const auto& block : blocks) {
        auto guardOrError = TMemoryUsageTrackerGuard::TryAcquire(
            memoryTracker,
            block.Size());
        if (!guardOrError.IsOK()) {
            Location_->ReportThrottledWrite();
            return MakeFuture<NIO::TIOCounters>(
                TError(guardOrError).SetCode(NChunkClient::EErrorCode::WriteThrottlingActive));
        }
        memoryTrackerGuards.push_back(std::move(guardOrError.Value()));
    }

    // Work around possible livelock. We might consume all memory allocated for blob sessions. That would block
    // all other PutBlock requests.
    std::vector<TFuture<void>> precedingBlockReceivedFutures;
    for (int precedingBlockIndex = WindowStartBlockIndex_; precedingBlockIndex < startBlockIndex; precedingBlockIndex++) {
        const auto& slot = GetSlot(precedingBlockIndex);
        if (slot.State == EBlobSessionSlotState::Empty) {
            precedingBlockReceivedFutures.push_back(slot.ReceivedPromise.ToFuture().ToUncancelable());
        }
    }

    if (precedingBlockReceivedFutures.empty()) {
        return DoPerformPutBlocks(startBlockIndex, blocks, std::move(memoryTrackerGuards), enableCaching);
    }

    return AllSucceeded(precedingBlockReceivedFutures)
        .WithTimeout(Config_->SessionBlockReorderTimeout)
        .Apply(BIND([config = Config_] (const TError& error) {
            if (error.GetCode() == NYT::EErrorCode::Timeout) {
                return TError(
                    NChunkClient::EErrorCode::WriteThrottlingActive,
                    "Block reordering timeout")
                    << TErrorAttribute("timeout", config->SessionBlockReorderTimeout);
            }

            return error;
        }))
        .Apply(BIND(&TBlobSession::DoPerformPutBlocks, MakeStrong(this), startBlockIndex, blocks, Passed(std::move(memoryTrackerGuards)), enableCaching)
            .AsyncVia(SessionInvoker_));
}

TFuture<NIO::TIOCounters> TBlobSession::DoPerformPutBlocks(
    int startBlockIndex,
    const std::vector<TBlock>& blocks,
    std::vector<TMemoryUsageTrackerGuard> memoryTrackerGuards,
    bool enableCaching)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    // Run the validation again since the context could have been switched since the last check.
    ValidateActive();

    const auto& blockCache = Bootstrap_->GetBlockCache();

    std::vector<int> receivedBlockIndexes;
    for (int localIndex = 0; localIndex < std::ssize(blocks); ++localIndex) {
        int blockIndex = startBlockIndex + localIndex;
        const auto& block = blocks[localIndex];
        TBlockId blockId(GetChunkId(), blockIndex);
        ValidateBlockIsInWindow(blockIndex);

        if (!Location_->HasEnoughSpace(block.Size())) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoLocationAvailable,
                "No enough space left on location");
        }

        auto& slot = GetSlot(blockIndex);
        if (slot.State != ESlotState::Empty) {
            if (TRef::AreBitwiseEqual(slot.Block.Data, block.Data)) {
                YT_LOG_WARNING("Skipped duplicate block (Block: %v)", blockIndex);
                continue;
            }

            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::BlockContentMismatch,
                "Block %v with a different content already received",
                blockId)
                << TErrorAttribute("window_start", WindowStartBlockIndex_);
        }

        ++BlockCount_;

        slot.State = ESlotState::Received;
        slot.Block = block;
        slot.ReceivedPromise.TrySet();

        if (enableCaching) {
            blockCache->PutBlock(blockId, EBlockType::CompressedData, block);
        }

        Location_->UpdateUsedSpace(block.Size());
        receivedBlockIndexes.push_back(blockIndex);
    }

    auto totalSize = GetByteSize(blocks);
    Size_ += totalSize;

    YT_LOG_DEBUG_UNLESS(receivedBlockIndexes.empty(), "Blocks received (Blocks: %v, TotalSize: %v)",
        receivedBlockIndexes,
        totalSize);

    // Organize blocks in packs of BytesPerWrite size and pass them to the pipeline.
    int firstBlockIndex = WindowIndex_;
    std::vector<TBlock> blocksToWrite;
    i64 blocksToWriteSize = 0;

    auto flushBlocksToPipeline = [&] {
        YT_VERIFY(std::ssize(blocksToWrite) == WindowIndex_ - firstBlockIndex);
        if (firstBlockIndex == WindowIndex_) {
            return;
        }

        Pipeline_->WriteBlocks(firstBlockIndex, blocksToWrite)
            .Subscribe(
                BIND(&TBlobSession::OnBlocksWritten, MakeStrong(this), firstBlockIndex, WindowIndex_)
                    .Via(SessionInvoker_));

        firstBlockIndex = WindowIndex_;
        blocksToWriteSize = 0;
        blocksToWrite.clear();
    };

    while (true) {
        if (WindowIndex_ >= std::ssize(Window_)) {
            flushBlocksToPipeline();
            break;
        }

        auto& slot = GetSlot(WindowIndex_);
        YT_VERIFY(slot.State == ESlotState::Received || slot.State == ESlotState::Empty);
        if (slot.State == ESlotState::Empty) {
            flushBlocksToPipeline();
            break;
        }

        slot.PendingIOGuard = Location_->AcquirePendingIO(
            std::move(memoryTrackerGuards[WindowIndex_ - startBlockIndex]),
            EIODirection::Write,
            Options_.WorkloadDescriptor,
            slot.Block.Size());

        if (auto error = slot.Block.ValidateChecksum(); !error.IsOK()) {
            auto blockId = TBlockId(GetChunkId(), WindowIndex_);
            SetFailed(error << TErrorAttribute("block_id", ToString(blockId)), /*fatal*/ false);
            return MakeFuture<NIO::TIOCounters>(Error_);
        }

        blocksToWrite.push_back(slot.Block);
        blocksToWriteSize += slot.Block.Size();
        ++WindowIndex_;

        if (blocksToWriteSize >= Config_->BytesPerWrite) {
            flushBlocksToPipeline();
        }
    }

    const auto& netThrottler = Bootstrap_->GetInThrottler(Options_.WorkloadDescriptor);
    const auto& diskThrottler = Location_->GetInThrottler(Options_.WorkloadDescriptor);
    return AllSucceeded(std::vector{
        netThrottler->Throttle(totalSize),
        diskThrottler->Throttle(totalSize)
    }).Apply(BIND([] {
        return NIO::TIOCounters{};
    }));
}

void TBlobSession::OnBlocksWritten(int beginBlockIndex, int endBlockIndex, const TError& error)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    if (Canceled_.load()) {
        return;
    }

    if (!error.IsOK()) {
        if (IsOutOfDiskSpaceError(error)) {
            SetFailed(
                TError(
                    NChunkClient::EErrorCode::NoSpaceLeftOnDevice,
                    "Not enough space to finish blob session for chunk %v",
                    GetChunkId())
                << error,
                /*fatal*/ false);
        } else {
            SetFailed(
                TError(
                    NChunkClient::EErrorCode::IOError,
                    "Error writing chunk %v",
                    GetChunkId())
                << error,
                /*fatal*/ true);
        }
        return;
    }

    for (int blockIndex = beginBlockIndex; blockIndex < endBlockIndex; ++blockIndex) {
        auto& slot = GetSlot(blockIndex);
        slot.MemoryUsageGuard = slot.PendingIOGuard.MoveMemoryTrackerGuard();
        slot.PendingIOGuard = {};
        if (error.IsOK()) {
            YT_VERIFY(slot.State == ESlotState::Received);
            slot.State = ESlotState::Written;
            slot.WrittenPromise.TrySet();
        }
    }
}

TFuture<TDataNodeServiceProxy::TRspPutBlocksPtr> TBlobSession::DoSendBlocks(
    int firstBlockIndex,
    int blockCount,
    const TNodeDescriptor& targetDescriptor)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    const auto& channelFactory = Bootstrap_
        ->GetClient()
        ->GetNativeConnection()
        ->GetChannelFactory();
    auto channel = channelFactory->CreateChannel(targetDescriptor.GetAddressOrThrow(Bootstrap_->GetLocalNetworks()));
    TDataNodeServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Config_->NodeRpcTimeout);

    auto req = proxy.PutBlocks();
    req->SetResponseHeavy(true);
    req->SetMultiplexingBand(EMultiplexingBand::Heavy);
    ToProto(req->mutable_session_id(), SessionId_);
    req->set_first_block_index(firstBlockIndex);

    i64 requestSize = 0;

    std::vector<TBlock> blocks;
    blocks.reserve(blockCount);
    for (int blockIndex = firstBlockIndex; blockIndex < firstBlockIndex + blockCount; ++blockIndex) {
        blocks.push_back(GetBlock(blockIndex));
        requestSize += blocks.back().Size();
    }
    SetRpcAttachedBlocks(req, blocks);

    const auto& throttler = Bootstrap_->GetOutThrottler(Options_.WorkloadDescriptor);
    return throttler->Throttle(requestSize).Apply(BIND([=] () {
        return req->Invoke();
    }));
}

TFuture<NIO::TIOCounters> TBlobSession::DoFlushBlocks(int blockIndex)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    if (!IsInWindow(blockIndex)) {
        YT_LOG_DEBUG("Blocks are already flushed (BlockIndex: %v)",
            blockIndex);
        return MakeFuture(NIO::TIOCounters{});
    }

    const auto& slot = GetSlot(blockIndex);
    if (slot.State == ESlotState::Empty) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::WindowError,
            "Attempt to flush an unreceived block %v",
            TBlockId(GetChunkId(), blockIndex));
    }

    // WrittenPromise is set in session invoker, hence no need for AsyncVia.
    return slot.WrittenPromise.ToFuture().Apply(
        BIND(&TBlobSession::OnBlockFlushed, MakeStrong(this), blockIndex));
}

NIO::TIOCounters TBlobSession::OnBlockFlushed(int blockIndex)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    if (Canceled_.load()) {
        return NIO::TIOCounters{};
    }

    ReleaseBlocks(blockIndex);

    // The data for blob chunks is flushed to disk only when the blob session is finished, so return empty IO counters here.
    return NIO::TIOCounters{};
}

void TBlobSession::DoCancel(const TError& error)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    MarkAllSlotsFailed(error);
    Abort();
}

void TBlobSession::Abort()
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    Pipeline_->Abort()
        .Subscribe(BIND(&TBlobSession::OnAborted, MakeStrong(this))
            .Via(SessionInvoker_));

    ReleaseSpace();
}

void TBlobSession::OnAborted(const TError& error)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    Finished_.Fire(Error_);

    if (!error.IsOK()) {
        SetFailed(
            TError(
                NChunkClient::EErrorCode::IOError,
                "Error aborting chunk %v",
                SessionId_)
            << error,
            /*fatal*/ true);
    }
}

void TBlobSession::ReleaseBlocks(int flushedBlockIndex)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);
    YT_VERIFY(WindowStartBlockIndex_ <= flushedBlockIndex);

    auto delayBeforeFree = Location_->GetDelayBeforeBlobSessionBlockFree();

    while (WindowStartBlockIndex_ <= flushedBlockIndex) {
        auto& slot = GetSlot(WindowStartBlockIndex_);
        YT_VERIFY(slot.State == ESlotState::Written);

        if (delayBeforeFree) {
            YT_LOG_DEBUG("Simulate delay before blob write session block free (BlockSize: %v, Delay: %v)", slot.Block.Size(), *delayBeforeFree);

            slot.Block = {};

            YT_UNUSED_FUTURE(BIND([=] (TMemoryUsageTrackerGuard guard) {
                TDelayedExecutor::WaitForDuration(*delayBeforeFree);
                guard.Release();
            })
            .AsyncVia(GetCurrentInvoker())
            .Run(std::move(slot.MemoryUsageGuard)));
        } else {
            slot.Block = {};
            slot.MemoryUsageGuard.Release();
        }

        slot.PendingIOGuard.Release();
        slot.WrittenPromise.Reset();
        slot.ReceivedPromise.Reset();
        ++WindowStartBlockIndex_;
    }

    YT_LOG_DEBUG("Released blocks (WindowStart: %v)",
        WindowStartBlockIndex_);
}

bool TBlobSession::IsInWindow(int blockIndex)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    return blockIndex >= WindowStartBlockIndex_;
}

void TBlobSession::ValidateBlockIsInWindow(int blockIndex)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    if (!IsInWindow(blockIndex)) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::WindowError,
            "Block %v is out of the window",
            TBlockId(GetChunkId(), blockIndex));
    }
}

TBlobSession::TSlot& TBlobSession::GetSlot(int blockIndex)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);
    YT_VERIFY(IsInWindow(blockIndex));

    while (std::ssize(Window_) <= blockIndex) {
        Window_.emplace_back();
        auto& slot = Window_.back();
        slot.WrittenPromise.OnCanceled(
            BIND(
                &TBlobSession::OnSlotCanceled,
                MakeWeak(this),
                WindowStartBlockIndex_ + std::ssize(Window_) - 1)
            .Via(SessionInvoker_));
    }

    return Window_[blockIndex];
}

TBlock TBlobSession::GetBlock(int blockIndex)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    ValidateBlockIsInWindow(blockIndex);

    Ping();

    const auto& slot = GetSlot(blockIndex);
    if (slot.State == ESlotState::Empty) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::WindowError,
            "Trying to retrieve block %v that is not received yet",
            TBlockId(GetChunkId(), blockIndex));
    }

    YT_LOG_DEBUG("Block retrieved (Block: %v)", blockIndex);

    return slot.Block;
}

void TBlobSession::MarkAllSlotsFailed(const TError& error)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    for (const auto& slot : Window_) {
        if (const auto& promise = slot.WrittenPromise) {
            promise.TrySet(error);
        }

        if (const auto& promise = slot.ReceivedPromise) {
            promise.TrySet(error);
        }
    }
}

void TBlobSession::ReleaseSpace()
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    Location_->UpdateUsedSpace(-Size_);
}

void TBlobSession::SetFailed(const TError& error, bool fatal)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    if (!Error_.IsOK()) {
        return;
    }

    Error_ = TError("Blob session failed")
        << TErrorAttribute("fatal", fatal)
        << error;
    YT_LOG_WARNING(error, "Blob session failed (Fatal: %v)",
        fatal);

    MarkAllSlotsFailed(error);

    if (fatal) {
        Location_->ScheduleDisable(Error_);
    }
}

void TBlobSession::OnSlotCanceled(int blockIndex, const TError& error)
{
    VERIFY_INVOKER_AFFINITY(SessionInvoker_);

    Cancel(TError(
        "Blob session canceled at block %v",
        TBlockId(GetChunkId(), blockIndex))
        << error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
