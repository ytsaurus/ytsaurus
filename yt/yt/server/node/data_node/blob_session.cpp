#include "blob_session.h"

#include "blob_chunk.h"
#include "bootstrap.h"
#include "chunk_store.h"
#include "config.h"
#include "location.h"

#include <yt/yt/server/node/cluster_node/public.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/io/chunk_file_reader.h>
#include <yt/yt/server/lib/io/chunk_file_writer.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/ring_queue.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NDataNode {

using namespace NChunkClient::NProto;
using namespace NChunkClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NIO;
using namespace NNodeTrackerClient;
using namespace NProfiling;
using namespace NRpc;

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

    TFuture<void> WriteBlocks(
        const IChunkWriter::TWriteBlocksOptions& options,
        int firstBlockIndex,
        std::vector<TBlock> blocks)
    {
        return EnqueueCommand(
            BIND(&TBlobWritePipeline::DoWriteBlocks, MakeStrong(this), options, firstBlockIndex, std::move(blocks)));
    }

    TFuture<void> Close(
        const IChunkWriter::TWriteBlocksOptions& options,
        TRefCountedChunkMetaPtr chunkMeta)
    {
        return EnqueueCommand(
            BIND(&TBlobWritePipeline::DoClose, MakeStrong(this), options, std::move(chunkMeta)));
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
        YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

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
        YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

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
        YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

        YT_VERIFY(CommandRunning_);
        CommandRunning_ = false;
        promise.Set(error);

        RunCommands();
    }


    TFuture<void> DoOpen()
    {
        YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

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

    TFuture<void> DoWriteBlocks(
        const IChunkWriter::TWriteBlocksOptions& options,
        int firstBlockIndex,
        const std::vector<TBlock>& blocks)
    {
        YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

        auto blockCount = std::ssize(blocks);
        auto totalSize = GetByteSize(blocks);

        YT_LOG_DEBUG("Started writing blocks (BlockIndexes: %v-%v, TotalSize: %v)",
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
            totalSize);

        TWallTimer timer;

        // This is how TFileWriter works.
        YT_VERIFY(!Writer_->WriteBlocks(options, Options_.WorkloadDescriptor, blocks));

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

    TFuture<void> DoClose(
        const IChunkWriter::TWriteBlocksOptions& options,
        const TRefCountedChunkMetaPtr& chunkMeta)
    {
        YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

        YT_LOG_DEBUG("Started closing chunk writer (ChunkSize: %v)",
            Writer_->GetDataSize());

        auto deferredChunkMeta = New<TDeferredChunkMeta>();
        deferredChunkMeta->MergeFrom(*chunkMeta);

        TWallTimer timer;

        return Writer_->Close(options, Options_.WorkloadDescriptor, deferredChunkMeta).Apply(
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
        YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

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
    TLockedChunkGuard lockedChunkGuard,
    IChunkWriter::TWriteBlocksOptions writeBlocksOptions)
    : TSessionBase(
        std::move(config),
        bootstrap,
        sessionId,
        options,
        std::move(location),
        std::move(lease),
        std::move(lockedChunkGuard),
        std::move(writeBlocksOptions))
    , Pipeline_(New<TBlobWritePipeline>(
        Location_,
        SessionInvoker_,
        SessionId_.ChunkId,
        Options_,
        Logger))
{ }

TFuture<void> TBlobSession::DoStart()
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    PendingBlockMemoryGuard_ = TMemoryUsageTrackerGuard::Build(Location_->GetWriteMemoryTracker());

    PendingBlockLocationMemoryGuard_ = Location_->AcquireLocationMemory(
        EIODirection::Write,
        Options_.WorkloadDescriptor,
        /*delta*/ 0);

    Pipeline_->Open()
        .Subscribe(BIND(&TBlobSession::OnStarted, MakeStrong(this))
            .Via(SessionInvoker_));

    // No need to wait for the writer to get opened.
    return VoidFuture;
}

void TBlobSession::OnStarted(const TError& error)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

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

TFuture<ISession::TFinishResult> TBlobSession::DoFinish(
    const TRefCountedChunkMetaPtr& chunkMeta,
    std::optional<int> blockCount)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);
    YT_VERIFY(chunkMeta);

    if (!blockCount) {
        return MakeFuture<TFinishResult>(TError("Attempt to finish a blob session %v without specifying block count",
            SessionId_));
    }

    auto currentBlockCount = BlockCount_.load();
    if (*blockCount != currentBlockCount) {
        return MakeFuture<TFinishResult>(TError("Block count mismatch in blob session %v: expected %v, got %v",
            SessionId_,
            currentBlockCount,
            *blockCount));
    }

    for (int blockIndex = WindowStartBlockIndex_; blockIndex < std::ssize(Window_); ++blockIndex) {
        const auto& slot = GetSlot(blockIndex);
        if (slot.State != ESlotState::Released) {
            return MakeFuture<TFinishResult>(TError(
                NChunkClient::EErrorCode::WindowError,
                "Attempt to finish a session with an unflushed block %v",
                TBlockId(GetChunkId(), blockIndex)));
        }
    }

    if (!Error_.IsOK()) {
        return MakeFuture<TFinishResult>(Error_);
    }

    return Pipeline_->Close(WriteBlocksOptions_, chunkMeta)
        .Apply(BIND(&TBlobSession::OnFinished, MakeStrong(this))
            .AsyncVia(SessionInvoker_));
}

i64 TBlobSession::GetMemoryUsage() const
{
    return MemoryUsage_.load();
}

i64 TBlobSession::GetTotalSize() const
{
    return TotalByteSize_.load();
}

i64 TBlobSession::GetBlockCount() const
{
    return BlockCount_.load();
}

i64 TBlobSession::GetWindowSize() const
{
    return WindowSize_.load();
}

i64 TBlobSession::GetIntermediateEmptyBlockCount() const
{
    return IntermediateEmptyBlockCount_.load();
}

ISession::TFinishResult TBlobSession::OnFinished(const TError& error)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

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

    return TFinishResult {
        .ChunkInfo = Pipeline_->GetChunkInfo(),
        .ChunkWriterStatistics = WriteBlocksOptions_.ClientOptions.ChunkWriterStatistics,
    };
}

TFuture<NIO::TIOCounters> TBlobSession::DoPutBlocks(
    int startBlockIndex,
    std::vector<TBlock> blocks,
    i64 cumulativeBlockSize,
    bool enableCaching)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    if (blocks.empty()) {
        return MakeFuture<NIO::TIOCounters>({});
    }

    bool useCumulativeBlockSize = (cumulativeBlockSize != 0);

    // Updating right boundary of cumulative block sizes. A block with following number was received.
    if (useCumulativeBlockSize && cumulativeBlockSize > MaxCumulativeBlockSize_) {
        // Delta is new allocated memory of block's window.
        auto deltaMaxCumulativeBlockSize = cumulativeBlockSize - MaxCumulativeBlockSize_;
        MaxCumulativeBlockSize_ = cumulativeBlockSize;

        // Add memory to guards.
        // This guard account memory in memory tracker.
        PendingBlockMemoryGuard_.IncreaseSize(deltaMaxCumulativeBlockSize);
        // This guard account memory in location - without memory tracker - for per location counting.
        PendingBlockLocationMemoryGuard_.IncreaseSize(deltaMaxCumulativeBlockSize);
    }

    std::vector<TFuture<void>> precedingBlockReceivedFutures;
    for (int precedingBlockIndex = WindowStartBlockIndex_; precedingBlockIndex < startBlockIndex; precedingBlockIndex++) {
        const auto& slot = GetSlot(precedingBlockIndex);
        if (slot.State == EBlobSessionSlotState::Empty) {
            precedingBlockReceivedFutures.push_back(slot.ReceivedPromise.ToFuture().ToUncancelable());
        }
    }

    i64 intermediateEmptyBlockCount = 0;
    for (int blockIndex = WindowStartBlockIndex_; blockIndex < std::ssize(Window_); blockIndex++) {
        const auto& slot = GetSlot(blockIndex);
        if (slot.State == EBlobSessionSlotState::Empty) {
            intermediateEmptyBlockCount++;
        }
    }

    IntermediateEmptyBlockCount_.store(intermediateEmptyBlockCount);

    if (precedingBlockReceivedFutures.empty()) {
        return DoPerformPutBlocks(
            startBlockIndex,
            std::move(blocks),
            useCumulativeBlockSize,
            enableCaching);
    }

    auto allPrecedingBlocksReceivedFuture = AllSucceeded(precedingBlockReceivedFutures)
        .WithTimeout(Config_->SessionBlockReorderTimeout)
        .Apply(BIND([config = Config_] (const TError& error) {
            if (error.GetCode() == NYT::EErrorCode::Timeout) {
                THROW_ERROR_EXCEPTION(
                    NChunkClient::EErrorCode::WriteThrottlingActive,
                    "Block reordering timeout")
                    << TErrorAttribute("timeout", config->SessionBlockReorderTimeout);
            }

            if (!error.IsOK()) {
                THROW_ERROR(error);
            }
        })
        .AsyncVia(SessionInvoker_));

    return allPrecedingBlocksReceivedFuture
        .Apply(BIND(
            &TBlobSession::DoPerformPutBlocks,
            MakeStrong(this),
            startBlockIndex,
            Passed(std::move(blocks)),
            useCumulativeBlockSize,
            enableCaching)
            .AsyncVia(SessionInvoker_));
}

TFuture<NIO::TIOCounters> TBlobSession::DoPerformPutBlocks(
    int startBlockIndex,
    std::vector<TBlock> blocks,
    bool useCumulativeBlockSize,
    bool enableCaching)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    // Run the validation again since the context could have been switched since the last check.
    ValidateActive();

    const auto& blockCache = Bootstrap_->GetBlockCache();

    const auto& memoryTracker = Location_->GetWriteMemoryTracker();
    std::vector<TLocationMemoryGuard> locationMemoryGuards;

    std::vector<int> receivedBlockIndexes;
    for (int localIndex = 0; localIndex < std::ssize(blocks); ++localIndex) {
        int blockIndex = startBlockIndex + localIndex;
        auto& block = blocks[localIndex];
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
                locationMemoryGuards.push_back({});
                continue;
            }

            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::BlockContentMismatch,
                "Block %v with a different content already received",
                blockId)
                << TErrorAttribute("window_start", WindowStartBlockIndex_);
        }

        BlockCount_.fetch_add(1);

        {
            auto blockSize = block.Size();

            if (useCumulativeBlockSize) {
                // Move tracking from pending guards.
                if (PendingBlockLocationMemoryGuard_.GetSize() >= blockSize) {
                    PendingBlockLocationMemoryGuard_.DecreaseSize(blockSize);
                } else {
                    YT_LOG_ALERT(
                        "Location memory guard is smaller than the size of the incoming block (ChunkId: %v)",
                        GetChunkId());
                }

                if (PendingBlockMemoryGuard_.GetSize() >= blockSize) {
                    PendingBlockMemoryGuard_.DecreaseSize(blockSize);
                } else {
                    YT_LOG_ALERT(
                        "Memory guard is smaller than the size of the incoming block (ChunkId: %v)",
                        GetChunkId());
                }
            }

            // Track memory per location - without memory tracker.
            locationMemoryGuards.push_back(Location_->AcquireLocationMemory(
                EIODirection::Write,
                Options_.WorkloadDescriptor,
                blockSize));

            // Track in memory tracker.
            block.Data = TrackMemory(memoryTracker, std::move(block.Data));

            // Counting for session manager orchid.
            MemoryUsage_.fetch_add(blockSize);
        }

        {
            slot.State = ESlotState::Received;
            slot.Block = block;
            slot.ReceivedPromise.TrySet();

            if (enableCaching) {
                blockCache->PutBlock(blockId, EBlockType::CompressedData, block);
            }

            Location_->UpdateUsedSpace(block.Size());
            receivedBlockIndexes.push_back(blockIndex);
        }
    }

    YT_VERIFY(blocks.size() == locationMemoryGuards.size());

    auto totalSize = GetByteSize(blocks);
    TotalByteSize_.fetch_add(totalSize);

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

        Pipeline_->WriteBlocks(WriteBlocksOptions_, firstBlockIndex, blocksToWrite)
            .Subscribe(
                BIND(&TBlobSession::OnBlocksWritten, MakeStrong(this), firstBlockIndex, WindowIndex_)
                    .Via(SessionInvoker_));

        firstBlockIndex = WindowIndex_;
        blocksToWriteSize = 0;
        blocksToWrite.clear();
    };

    auto bytesPerWrite = Bootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->BytesPerWrite.value_or(Config_->BytesPerWrite);

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

        slot.LocationMemoryGuard = std::move(locationMemoryGuards[WindowIndex_ - startBlockIndex]);
        slot.PendingIOGuard = Location_->AcquirePendingIO(
            {},
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

        if (blocksToWriteSize >= bytesPerWrite) {
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
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

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

            // Checking for a state is necessary because the subscriber may trigger after calling slot.WrittenPromise.TrySet().
            if (Options_.DisableSendBlocks && slot.State == ESlotState::Written) {
                YT_VERIFY(slot.MemoryUsageGuard.GetSize() == 0);
                ReleaseBlockSlot(slot);
            }
        }
    }
}

TFuture<TDataNodeServiceProxy::TRspPutBlocksPtr> TBlobSession::DoSendBlocks(
    int firstBlockIndex,
    int blockCount,
    i64 cumulativeBlockSize,
    const TNodeDescriptor& targetDescriptor)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

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
    req->set_cumulative_block_size(cumulativeBlockSize);

    i64 requestSize = 0;

    std::vector<TBlock> blocks;
    blocks.reserve(blockCount);
    for (int blockIndex = firstBlockIndex; blockIndex < firstBlockIndex + blockCount; ++blockIndex) {
        blocks.push_back(GetBlock(blockIndex));
        requestSize += blocks.back().Size();
    }
    SetRpcAttachedBlocks(req, blocks);

    const auto& throttler = Bootstrap_->GetOutThrottler(Options_.WorkloadDescriptor);
    return throttler->Throttle(requestSize).Apply(BIND([=] {
        return req->Invoke();
    }));
}

TFuture<ISession::TFlushBlocksResult> TBlobSession::DoFlushBlocks(int blockIndex)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    if (!IsInWindow(blockIndex)) {
        YT_LOG_DEBUG("Blocks are already flushed (BlockIndex: %v)",
            blockIndex);
        return MakeFuture(TFlushBlocksResult {
            .IOCounters = {},
            .ChunkWriterStatistics = New<NChunkClient::TChunkWriterStatistics>(),
        });
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

ISession::TFlushBlocksResult TBlobSession::OnBlockFlushed(int blockIndex)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    auto flushBlocksResult = TFlushBlocksResult {
        .IOCounters = {},
        .ChunkWriterStatistics = New<NChunkClient::TChunkWriterStatistics>(),
    };

    if (Canceled_.load()) {
        return flushBlocksResult;
    }

    ReleaseBlocks(blockIndex);

    // The data for blob chunks is flushed to disk only when the blob session is finished, so return empty IO counters here.
    return flushBlocksResult;
}

void TBlobSession::DoCancel(const TError& error)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    MarkAllSlotsFailed(error);
    Abort();
}

void TBlobSession::Abort()
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    Pipeline_->Abort()
        .Subscribe(BIND(&TBlobSession::OnAborted, MakeStrong(this))
            .Via(SessionInvoker_));

    ReleaseSpace();
}

void TBlobSession::OnAborted(const TError& error)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

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

void TBlobSession::ReleaseBlockSlot(TSlot& slot)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    MemoryUsage_.fetch_sub(slot.Block.Size());
    slot.Block = {};
    slot.MemoryUsageGuard.Release();
    slot.LocationMemoryGuard.Release();
    slot.State = EBlobSessionSlotState::Released;
}

void TBlobSession::ReleaseBlocks(int flushedBlockIndex)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);
    YT_VERIFY(WindowStartBlockIndex_ <= flushedBlockIndex);

    auto delayBeforeFree = Location_->GetDelayBeforeBlobSessionBlockFree();

    while (WindowStartBlockIndex_ <= flushedBlockIndex) {
        auto& slot = GetSlot(WindowStartBlockIndex_);
        YT_VERIFY(slot.State == ESlotState::Written || slot.State == EBlobSessionSlotState::Released);

        if (slot.State == ESlotState::Written) {
            YT_VERIFY(slot.MemoryUsageGuard.GetSize() == 0);

            if (delayBeforeFree) {
                YT_LOG_DEBUG("Simulate delay before blob write session block free (BlockSize: %v, Delay: %v)", slot.Block.Size(), *delayBeforeFree);

                YT_UNUSED_FUTURE(BIND([=] (TBlock block) {
                    TDelayedExecutor::WaitForDuration(*delayBeforeFree);
                    block = {};
                })
                .AsyncVia(GetCurrentInvoker())
                .Run(std::move(slot.Block)));

                slot.State = EBlobSessionSlotState::Released;
            } else {
                ReleaseBlockSlot(slot);
            }
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
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    return blockIndex >= WindowStartBlockIndex_;
}

void TBlobSession::ValidateBlockIsInWindow(int blockIndex)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    if (!IsInWindow(blockIndex)) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::WindowError,
            "Block %v is out of the window",
            TBlockId(GetChunkId(), blockIndex));
    }
}

TBlobSession::TSlot& TBlobSession::GetSlot(int blockIndex)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);
    YT_VERIFY(IsInWindow(blockIndex));

    while (std::ssize(Window_) <= blockIndex) {
        Window_.emplace_back();
        WindowSize_.fetch_add(1);

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
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    ValidateBlockIsInWindow(blockIndex);

    Ping();

    const auto& slot = GetSlot(blockIndex);
    if (slot.State == ESlotState::Empty) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::WindowError,
            "Trying to retrieve block %v that is not received yet",
            TBlockId(GetChunkId(), blockIndex));
    }

    if (slot.State == ESlotState::Released) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::WindowError,
            "Trying to retrieve block %v that is already released",
            TBlockId(GetChunkId(), blockIndex));
    }

    YT_LOG_DEBUG("Block retrieved (Block: %v)", blockIndex);

    return slot.Block;
}

void TBlobSession::MarkAllSlotsFailed(const TError& error)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

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
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    Location_->UpdateUsedSpace(-TotalByteSize_.load());
}

void TBlobSession::SetFailed(const TError& error, bool fatal)
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

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
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    Cancel(TError(
        "Blob session canceled at block %v",
        TBlockId(GetChunkId(), blockIndex))
        << error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
