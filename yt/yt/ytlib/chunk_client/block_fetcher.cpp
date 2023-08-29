#include "block_fetcher.h"

#include "private.h"
#include "block_cache.h"
#include "config.h"
#include "dispatcher.h"
#include "chunk_reader_memory_manager.h"
#include "chunk_reader_statistics.h"

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/memory_reference_tracker.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

TBlockFetcher::TBlockFetcher(
    TBlockFetcherConfigPtr config,
    std::vector<TBlockInfo> blockInfos,
    TChunkReaderMemoryManagerPtr memoryManager,
    std::vector<IChunkReaderPtr> chunkReaders,
    IBlockCachePtr blockCache,
    NCompression::ECodec codecId,
    double compressionRatio,
    const TClientChunkReadOptions& chunkReadOptions,
    IInvokerPtr sessionInvoker)
    : Config_(std::move(config))
    , BlockInfos_(std::move(blockInfos))
    , BlockCache_(std::move(blockCache))
    , SessionInvoker_(std::move(sessionInvoker))
    , CompressionInvoker_(SessionInvoker_
        ? SessionInvoker_
        : GetCompressionInvoker(chunkReadOptions.WorkloadDescriptor))
    , ReaderInvoker_(CreateSerializedInvoker(SessionInvoker_
        ? SessionInvoker_
        : TDispatcher::Get()->GetReaderInvoker()))
    , CompressionRatio_(compressionRatio)
    , MemoryManager_(std::move(memoryManager))
    , Codec_(NCompression::GetCodec(codecId))
    , ChunkReadOptions_(chunkReadOptions)
    , Logger(ChunkClientLogger)
    , Chunks_(chunkReaders.size())
{
    YT_VERIFY(!chunkReaders.empty());
    YT_VERIFY(BlockCache_);
    YT_VERIFY(!BlockInfos_.empty());

    for (int readerIndex = 0; readerIndex < std::ssize(chunkReaders); ++readerIndex) {
        Chunks_[readerIndex].Reader = std::move(chunkReaders[readerIndex]);
    }

    if (ChunkReadOptions_.ReadSessionId) {
        Logger.AddTag("ReadSessionId: %v",
            ChunkReadOptions_.ReadSessionId);
    }

    auto getBlockDescriptor = [&] (const TBlockInfo& blockInfo) {
        return TBlockDescriptor{
            .ReaderIndex = blockInfo.ReaderIndex,
            .BlockIndex = blockInfo.BlockIndex
        };
    };

    std::sort(
        BlockInfos_.begin(),
        BlockInfos_.end(),
        [&] (const TBlockInfo& lhs, const TBlockInfo& rhs) {
            return
                std::make_tuple(lhs.Priority, lhs.ReaderIndex, lhs.BlockIndex) <
                std::make_tuple(rhs.Priority, rhs.ReaderIndex, rhs.BlockIndex);
        });

    YT_VERIFY(std::all_of(BlockInfos_.begin(), BlockInfos_.end(), [] (const TBlockInfo& info) {
        return
            info.BlockType == EBlockType::None ||
            info.BlockType == EBlockType::UncompressedData;
    }));

    int windowSize = 1;
    i64 totalRemainingSize = 0;
    for (int index = 0; index + 1 < std::ssize(BlockInfos_); ++index) {
        if (getBlockDescriptor(BlockInfos_[index]) != getBlockDescriptor(BlockInfos_[index + 1])) {
            ++windowSize;
        }
        totalRemainingSize += BlockInfos_[index].UncompressedDataSize;
        ++Chunks_[BlockInfos_[index].ReaderIndex].RemainingBlockCount;
    }
    totalRemainingSize += BlockInfos_.back().UncompressedDataSize;
    ++Chunks_[BlockInfos_.back().ReaderIndex].RemainingBlockCount;

    Window_ = std::make_unique<TWindowSlot[]>(windowSize);
    TotalRemainingFetches_ = BlockInfos_.size();
    TotalRemainingSize_ = totalRemainingSize;

    // We consider contiguous segments consisting of the same block and store them
    // in the BlockIndexToWindowIndex hashmap.
    // [leftIndex, rightIndex) is a half-interval containing all blocks
    // equal to BlockInfos[leftIndex].
    // We also explicitly unique the elements of BlockInfos_.
    std::vector<TBlockDescriptor> blockDescriptors;
    blockDescriptors.reserve(windowSize);
    i64 totalBlockUncompressedSize = 0;
    int windowIndex = 0;
    for (int leftIndex = 0, rightIndex = 0; leftIndex != std::ssize(BlockInfos_); leftIndex = rightIndex) {
        auto& currentBlock = BlockInfos_[leftIndex];
        while (
            rightIndex != std::ssize(BlockInfos_) &&
            getBlockDescriptor(BlockInfos_[rightIndex]) == getBlockDescriptor(currentBlock))
        {
            YT_VERIFY(BlockInfos_[rightIndex].BlockType == currentBlock.BlockType);
            ++rightIndex;
        }

        auto& currentChunk = Chunks_[currentBlock.ReaderIndex];
        if (currentChunk.BlockIndexToWindowIndex.contains(currentBlock.BlockIndex)) {
            auto windowIndex = GetOrCrash(currentChunk.BlockIndexToWindowIndex, currentBlock.BlockIndex);
            Window_[windowIndex].RemainingFetches += rightIndex - leftIndex;
        } else {
            currentChunk.BlockIndexToWindowIndex[currentBlock.BlockIndex] = windowIndex;
            Window_[windowIndex].RemainingFetches = rightIndex - leftIndex;
            blockDescriptors.push_back(getBlockDescriptor(currentBlock));
            totalBlockUncompressedSize += currentBlock.UncompressedDataSize;
            if (windowIndex != leftIndex) {
                BlockInfos_[windowIndex] = std::move(currentBlock);
            }
            ++windowIndex;
        }
    }
    YT_VERIFY(windowIndex == windowSize);

    // Now Window_ and BlockInfos_ correspond to each other.
    BlockInfos_.resize(windowSize);

    MemoryManager_->SetTotalSize(totalBlockUncompressedSize + Config_->WindowSize);
    MemoryManager_->SetPrefetchMemorySize(std::min(Config_->WindowSize, totalRemainingSize));

    TStringBuilder builder;
    bool first = true;
    for (const auto& blockDescriptor : blockDescriptors) {
        builder.AppendString(first ? "[" : ", ");
        first = false;

        auto chunkId = Chunks_[blockDescriptor.ReaderIndex].Reader->GetChunkId();
        builder.AppendFormat("%v:%v", chunkId, blockDescriptor.BlockIndex);
    }
    builder.AppendChar(']');

    YT_LOG_DEBUG("Creating block fetcher (Blocks: %v)", builder.Flush());

    YT_VERIFY(totalRemainingSize > 0);
}

TBlockFetcher::~TBlockFetcher()
{
    MemoryManager_->Finalize();
}

void TBlockFetcher::Start()
{
    YT_VERIFY(!Started_.exchange(true));

    FetchNextGroupMemoryFuture_ =
        MemoryManager_->AsyncAcquire(
            std::min(TotalRemainingSize_.load(), Config_->GroupSize));
    FetchNextGroupMemoryFuture_.Subscribe(BIND(
        &TBlockFetcher::FetchNextGroup,
            MakeWeak(this))
        .Via(ReaderInvoker_));
}

void TBlockFetcher::DoStartBlock(const TBlockInfo& blockInfo)
{
    TotalRemainingSize_ -= blockInfo.UncompressedDataSize;
}

void TBlockFetcher::DoFinishBlock(const TBlockInfo& blockInfo)
{
    i64 currentRemainingBlocks = Chunks_[blockInfo.ReaderIndex].RemainingBlockCount.fetch_sub(1) - 1;
    if (currentRemainingBlocks == 0) {
        OnReaderFinished_.Fire(blockInfo.ReaderIndex);
    }
}

void TBlockFetcher::DoSetBlock(const TBlockInfo& blockInfo, TWindowSlot& windowSlot, TBlock block)
{
    // Note that the last call to OnReaderFinished_ happens-before the end of the last FetchBlock(),
    // so call DoFinishBlock() before setting the promise. Otherwise, we can set the promise from
    // background fetch loop and then switch to the fiber calling the last FinishBlock(), leading to
    // violation of this guarantee.
    DoFinishBlock(blockInfo);
    GetBlockPromise(windowSlot).Set(std::move(block));
}

void TBlockFetcher::DoSetError(const TBlockInfo& blockInfo, TWindowSlot& windowSlot, const TError& error)
{
    // Do the same as in DoSetBlock(). See the comment above.
    DoFinishBlock(blockInfo);
    GetBlockPromise(windowSlot).Set(error);
}

void TBlockFetcher::SetTraceContextForReader(int readerIndex, TTraceContextPtr traceContext)
{
    YT_VERIFY(readerIndex < std::ssize(Chunks_));
    Chunks_[readerIndex].TraceContext.emplace(std::move(traceContext));
}

bool TBlockFetcher::HasMoreBlocks() const
{
    return TotalRemainingFetches_ > 0;
}

i64 TBlockFetcher::GetBlockSize(int readerIndex, int blockIndex) const
{
    int windowIndex = GetOrCrash(Chunks_[readerIndex].BlockIndexToWindowIndex, blockIndex);
    return BlockInfos_[windowIndex].UncompressedDataSize;
}

i64 TBlockFetcher::GetBlockSize(int blockIndex) const
{
    YT_VERIFY(std::ssize(Chunks_) == 1);
    return GetBlockSize(/*readerIndex*/ 0, blockIndex);
}

TFuture<TBlock> TBlockFetcher::FetchBlock(int readerIndex, int blockIndex)
{
    YT_VERIFY(Started_);
    YT_VERIFY(HasMoreBlocks());

    int windowIndex = GetOrCrash(Chunks_[readerIndex].BlockIndexToWindowIndex, blockIndex);
    auto& windowSlot = Window_[windowIndex];

    auto blockPromise = GetBlockPromise(windowSlot);

    YT_VERIFY(windowSlot.RemainingFetches > 0);
    if (!windowSlot.FetchStarted.test_and_set()) {
        auto chunkId = Chunks_[readerIndex].Reader->GetChunkId();

        YT_LOG_DEBUG("Fetching block out of turn "
            "(ChunkId: %v, BlockIndex: %v, WindowIndex: %v)",
            chunkId,
            blockIndex,
            windowIndex);

        const auto& blockInfo = BlockInfos_[windowIndex];
        windowSlot.MemoryUsageGuard = MemoryManager_->Acquire(
            blockInfo.UncompressedDataSize);

        TBlockId blockId(chunkId, blockIndex);

        auto cachedBlock = Config_->UseUncompressedBlockCache
            ? BlockCache_->FindBlock(blockId, blockInfo.BlockType)
            : TCachedBlock();
        if (cachedBlock) {
            ChunkReadOptions_.ChunkReaderStatistics->DataBytesReadFromCache.fetch_add(
                cachedBlock.Size(),
                std::memory_order::relaxed);

            cachedBlock.Data = TrackMemory(ChunkReadOptions_.MemoryReferenceTracker, std::move(cachedBlock.Data));

            TRef ref = cachedBlock.Data;
            windowSlot.MemoryUsageGuard->CaptureBlock(std::move(cachedBlock.Data));

            cachedBlock = TBlock(TSharedRef(ref, MakeSharedRangeHolder(std::move(windowSlot.MemoryUsageGuard))));

            DoStartBlock(blockInfo);
            DoSetBlock(blockInfo, windowSlot, std::move(cachedBlock));
        } else {
            TBlockDescriptor blockDescriptor{
                .ReaderIndex = readerIndex,
                .BlockIndex = blockIndex
            };
            ReaderInvoker_->Invoke(BIND(
                &TBlockFetcher::RequestBlocks,
                MakeWeak(this),
                std::vector{windowIndex},
                std::vector{blockDescriptor}));
        }
    }

    auto blockFuture = blockPromise.ToFuture();
    if (--windowSlot.RemainingFetches == 0 && blockFuture.IsSet()) {
        ReaderInvoker_->Invoke(
            BIND(&TBlockFetcher::ReleaseBlocks,
                MakeWeak(this),
                std::vector{windowIndex}));
    }

    --TotalRemainingFetches_;

    return blockFuture;
}

TFuture<TBlock> TBlockFetcher::FetchBlock(int blockIndex)
{
    YT_VERIFY(std::ssize(Chunks_) == 1);
    return FetchBlock(/*readerIndex*/ 0, blockIndex);
}

void TBlockFetcher::DecompressBlocks(
    std::vector<int> windowIndexes,
    std::vector<TBlock> compressedBlocks)
{
    YT_VERIFY(windowIndexes.size() == compressedBlocks.size());

    std::vector<int> windowIndexesToRelease;
    for (int i = 0; i < std::ssize(compressedBlocks); ++i) {
        auto& compressedBlock = compressedBlocks[i];
        auto compressedBlockSize = compressedBlock.Size();
        int windowIndex = windowIndexes[i];
        const auto& blockInfo = BlockInfos_[windowIndex];
        auto readerIndex = blockInfo.ReaderIndex;
        auto blockIndex = blockInfo.BlockIndex;

        auto chunkId = Chunks_[readerIndex].Reader->GetChunkId();
        TBlockId blockId(chunkId, blockIndex);

        TSharedRef uncompressedBlock;
        if (Codec_->GetId() == NCompression::ECodec::None) {
            uncompressedBlock = std::move(compressedBlock.Data);
        } else {
            YT_LOG_DEBUG("Started decompressing block "
                "(ChunkId: %v, BlockIndex: %v, WindowIndex: %v, Codec: %v)",
                chunkId,
                blockIndex,
                windowIndex,
                Codec_->GetId());

            {
                TWallTimer timer;
                uncompressedBlock = Codec_->Decompress(compressedBlock.Data);
                DecompressionTime_ += timer.GetElapsedValue();
                YT_VERIFY(std::ssize(uncompressedBlock) == blockInfo.UncompressedDataSize);
            }

            YT_LOG_DEBUG("Finished decompressing block "
                "(ChunkId: %v, BlockIndex: %v, WindowIndex: %v, CompressedSize: %v, UncompressedSize: %v, Codec: %v)",
                chunkId,
                blockIndex,
                windowIndex,
                compressedBlock.Size(),
                uncompressedBlock.Size(),
                Codec_->GetId());
        }

        if (Config_->UseUncompressedBlockCache) {
            BlockCache_->PutBlock(
                blockId,
                blockInfo.BlockType,
                TBlock(uncompressedBlock));
        }

        UncompressedDataSize_ += uncompressedBlock.Size();
        CompressedDataSize_ += compressedBlockSize;

        uncompressedBlock = TrackMemory(
            ChunkReadOptions_.MemoryReferenceTracker,
            std::move(uncompressedBlock));

        auto& windowSlot = Window_[windowIndex];

        TRef ref = uncompressedBlock;
        windowSlot.MemoryUsageGuard->CaptureBlock(std::move(uncompressedBlock));

        uncompressedBlock = TSharedRef(
            ref,
            MakeSharedRangeHolder(std::move(windowSlot.MemoryUsageGuard)));

        DoSetBlock(blockInfo, windowSlot, TBlock(std::move(uncompressedBlock)));
        if (windowSlot.RemainingFetches == 0) {
            windowIndexesToRelease.push_back(windowIndex);
        }
    }

    if (!windowIndexesToRelease.empty()) {
        ReaderInvoker_->Invoke(
            BIND(&TBlockFetcher::ReleaseBlocks,
                MakeWeak(this),
                std::move(windowIndexesToRelease)));
    }
}

void TBlockFetcher::FetchNextGroup(const TErrorOr<TMemoryUsageGuardPtr>& memoryUsageGuardOrError)
{
    if (!memoryUsageGuardOrError.IsOK()) {
        YT_LOG_INFO(memoryUsageGuardOrError,
            "Failed to acquire memory in chunk reader memory manager");
        return;
    }

    const auto& memoryUsageGuard = memoryUsageGuardOrError.Value();
    auto* underlyingGuard = memoryUsageGuard->GetGuard();

    std::vector<int> windowIndexes;
    std::vector<TBlockDescriptor> blockDescriptors;
    i64 uncompressedSize = 0;
    i64 availableSlots = underlyingGuard->GetSlots();

    while (FirstUnfetchedWindowIndex_ < std::ssize(BlockInfos_)) {
        const auto& blockInfo = BlockInfos_[FirstUnfetchedWindowIndex_];
        auto readerIndex = blockInfo.ReaderIndex;
        auto blockIndex = blockInfo.BlockIndex;
        auto chunkId = Chunks_[readerIndex].Reader->GetChunkId();

        if (windowIndexes.empty() || uncompressedSize + blockInfo.UncompressedDataSize <= availableSlots) {
            if (Window_[FirstUnfetchedWindowIndex_].FetchStarted.test_and_set()) {
                // This block has been already requested out of order.
                YT_LOG_DEBUG("Skipping out of turn block (ChunkId: %v, BlockIndex: %v, WindowIndex: %v)",
                    chunkId,
                    blockIndex,
                    FirstUnfetchedWindowIndex_);
                ++FirstUnfetchedWindowIndex_;
                continue;
            }

            auto transferred = underlyingGuard->TransferSlots(
                std::min(
                    static_cast<i64>(blockInfo.UncompressedDataSize),
                    underlyingGuard->GetSlots()));
            Window_[FirstUnfetchedWindowIndex_].MemoryUsageGuard = New<TMemoryUsageGuard>(
                std::move(transferred),
                memoryUsageGuard->GetMemoryManager());

            TBlockId blockId(chunkId, blockIndex);
            auto cachedBlock = Config_->UseUncompressedBlockCache
                ? BlockCache_->FindBlock(blockId, blockInfo.BlockType)
                : TCachedBlock();
            if (cachedBlock) {
                ChunkReadOptions_.ChunkReaderStatistics->DataBytesReadFromCache.fetch_add(
                    cachedBlock.Size(),
                    std::memory_order::relaxed);

                cachedBlock.Data = TrackMemory(ChunkReadOptions_.MemoryReferenceTracker, std::move(cachedBlock.Data));

                auto& windowSlot = Window_[FirstUnfetchedWindowIndex_];

                TRef ref = cachedBlock.Data;
                windowSlot.MemoryUsageGuard->CaptureBlock(std::move(cachedBlock.Data));
                cachedBlock = TBlock(TSharedRef(
                    ref,
                    MakeSharedRangeHolder(std::move(windowSlot.MemoryUsageGuard))));

                DoStartBlock(blockInfo);
                DoSetBlock(blockInfo, windowSlot, std::move(cachedBlock));
            } else {
                uncompressedSize += blockInfo.UncompressedDataSize;
                windowIndexes.push_back(FirstUnfetchedWindowIndex_);
                blockDescriptors.push_back(TBlockDescriptor{
                    .ReaderIndex = readerIndex,
                    .BlockIndex = blockIndex
                });
            }
        } else {
            break;
        }

        ++FirstUnfetchedWindowIndex_;
    }

    if (windowIndexes.empty()) {
        FetchingCompleted_ = true;
        MemoryManager_->Finalize();
        return;
    }

    if (TotalRemainingSize_ > 0) {
        auto nextGroupSize = std::min<i64>(TotalRemainingSize_, Config_->GroupSize);
        MemoryManager_->SetPrefetchMemorySize(nextGroupSize);
        FetchNextGroupMemoryFuture_ = MemoryManager_->AsyncAcquire(nextGroupSize);
        FetchNextGroupMemoryFuture_.Subscribe(BIND(
            &TBlockFetcher::FetchNextGroup,
                MakeWeak(this))
            .Via(ReaderInvoker_));
    }

    RequestBlocks(
        std::move(windowIndexes),
        std::move(blockDescriptors));
}

void TBlockFetcher::MarkFailedBlocks(const std::vector<int>& windowIndexes, const TError& error)
{
    for (auto index : windowIndexes) {
        DoSetError(BlockInfos_[index], Window_[index], error);
    }
}

void TBlockFetcher::ReleaseBlocks(const std::vector<int>& windowIndexes)
{
    YT_LOG_DEBUG("Releasing blocks (WindowIndexes: %v)",
        MakeShrunkFormattableView(windowIndexes, TDefaultFormatter(), 3));

    for (auto index : windowIndexes) {
        ResetBlockPromise(Window_[index]);
    }
}

TPromise<TBlock> TBlockFetcher::GetBlockPromise(TWindowSlot& windowSlot)
{
    auto guard = Guard(windowSlot.BlockPromiseLock);
    if (!windowSlot.BlockPromise) {
        windowSlot.BlockPromise = NewPromise<TBlock>();
    }
    return windowSlot.BlockPromise;
}

void TBlockFetcher::ResetBlockPromise(TWindowSlot& windowSlot)
{
    TPromise<TBlock> promise;
    {
        auto guard = Guard(windowSlot.BlockPromiseLock);
        promise = std::move(windowSlot.BlockPromise);
    }
}

void TBlockFetcher::RequestBlocks(
    std::vector<int> windowIndexes,
    std::vector<TBlockDescriptor> blockDescriptors)
{
    YT_VERIFY(windowIndexes.size() == blockDescriptors.size());

    i64 uncompressedSize = 0;
    for (auto index : windowIndexes) {
        DoStartBlock(BlockInfos_[index]);
        uncompressedSize += BlockInfos_[index].UncompressedDataSize;
    }

    THashMap<int, std::vector<int>> readerIndexToBlockIndices;
    THashMap<int, std::vector<int>> readerIndexToWindowIndices;
    for (int index = 0; index < std::ssize(blockDescriptors); ++index) {
        const auto& blockDescriptor = blockDescriptors[index];
        auto readerIndex = blockDescriptor.ReaderIndex;
        readerIndexToBlockIndices[readerIndex].push_back(blockDescriptor.BlockIndex);
        readerIndexToWindowIndices[readerIndex].push_back(windowIndexes[index]);
    }

    for (auto& indexPair : readerIndexToBlockIndices) {
        auto readerIndex = indexPair.first;
        auto blockIndices = std::move(indexPair.second);
        const auto& chunkReader = Chunks_[readerIndex].Reader;

        YT_LOG_DEBUG("Requesting block group "
            "(ChunkId: %v, Blocks: %v, UncompressedSize: %v, CompressionRatio: %v)",
            chunkReader->GetChunkId(),
            MakeShrunkFormattableView(blockIndices, TDefaultFormatter(), 3),
            uncompressedSize,
            CompressionRatio_);

        auto future = [&] {
            std::optional<TTraceContextGuard> maybeGuard;
            if (Chunks_[readerIndex].TraceContext.has_value()) {
                maybeGuard.emplace(*Chunks_[readerIndex].TraceContext);
            }

            return chunkReader->ReadBlocks(
                IChunkReader::TReadBlocksOptions{
                    .ClientOptions = ChunkReadOptions_,
                    .EstimatedSize = static_cast<i64>(uncompressedSize * CompressionRatio_),
                    .SessionInvoker = SessionInvoker_,
                },
                blockIndices);
        }();

        // NB: Handling |OnGotBlocks| in an arbitrary thread seems OK.
        YT_VERIFY(readerIndexToWindowIndices[readerIndex].size() == blockIndices.size());
        future.SubscribeUnique(
            BIND(
                &TBlockFetcher::OnGotBlocks,
                MakeWeak(this),
                readerIndex,
                Passed(std::move(readerIndexToWindowIndices[readerIndex])),
                Passed(std::move(blockIndices))));
    }
}

void TBlockFetcher::OnGotBlocks(
    int readerIndex,
    std::vector<int> windowIndexes,
    std::vector<int> blockIndexes,
    TErrorOr<std::vector<TBlock>>&& blocksOrError)
{
    if (!blocksOrError.IsOK()) {
        MarkFailedBlocks(windowIndexes, blocksOrError);
        return;
    }

    auto blocks = std::move(blocksOrError.Value());

    YT_VERIFY(blocks.size() == blockIndexes.size());
    YT_VERIFY(blocks.size() == windowIndexes.size());

    auto chunkId = Chunks_[readerIndex].Reader->GetChunkId();
    YT_LOG_DEBUG("Got block group (ChunkId: %v, Blocks: %v)",
        chunkId,
        MakeShrunkFormattableView(blockIndexes, TDefaultFormatter(), 3));

    if (Codec_->GetId() == NCompression::ECodec::None) {
        DecompressBlocks(
            std::move(windowIndexes),
            std::move(blocks));
    } else {
        CompressionInvoker_->Invoke(BIND(
            &TBlockFetcher::DecompressBlocks,
            MakeWeak(this),
            Passed(std::move(windowIndexes)),
            Passed(std::move(blocks))));
    }
}

bool TBlockFetcher::IsFetchingCompleted() const
{
    return FetchingCompleted_;
}

i64 TBlockFetcher::GetUncompressedDataSize() const
{
    return UncompressedDataSize_;
}

i64 TBlockFetcher::GetCompressedDataSize() const
{
    return CompressedDataSize_;
}

TCodecDuration TBlockFetcher::GetDecompressionTime() const
{
    return TCodecDuration{
        Codec_->GetId(),
        NProfiling::ValueToDuration(DecompressionTime_)
    };
}

////////////////////////////////////////////////////////////////////////////////

TSequentialBlockFetcher::TSequentialBlockFetcher(
    TBlockFetcherConfigPtr config,
    std::vector<TBlockInfo> blockInfos,
    TChunkReaderMemoryManagerPtr memoryManager,
    std::vector<IChunkReaderPtr> chunkReaders,
    IBlockCachePtr blockCache,
    NCompression::ECodec codecId,
    double compressionRatio,
    const TClientChunkReadOptions& chunkReadOptions,
    IInvokerPtr sessionInvoker)
    : TBlockFetcher(
        std::move(config),
        blockInfos,
        std::move(memoryManager),
        std::move(chunkReaders),
        std::move(blockCache),
        codecId,
        compressionRatio,
        chunkReadOptions,
        std::move(sessionInvoker))
    , OriginalOrderBlockInfos_(std::move(blockInfos))
{ }

TFuture<TBlock> TSequentialBlockFetcher::FetchNextBlock()
{
    YT_VERIFY(CurrentIndex_ < std::ssize(OriginalOrderBlockInfos_));
    const auto& blockInfo = OriginalOrderBlockInfos_[CurrentIndex_++];
    return FetchBlock(blockInfo.ReaderIndex, blockInfo.BlockIndex);
}

i64 TSequentialBlockFetcher::GetNextBlockSize() const
{
    YT_VERIFY(CurrentIndex_ < std::ssize(OriginalOrderBlockInfos_));
    return OriginalOrderBlockInfos_[CurrentIndex_].UncompressedDataSize;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
