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

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/memory/memory_usage_tracker.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

TBlockFetcher::TBlockFetcher(
    TBlockFetcherConfigPtr config,
    std::vector<TBlockInfo> blockInfos,
    TChunkReaderMemoryManagerHolderPtr memoryManagerHolder,
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
    , MemoryManagerHolder_(std::move(memoryManagerHolder))
    , Codec_(NCompression::GetCodec(codecId))
    , ChunkReadOptions_(chunkReadOptions)
    , Logger(ChunkClientLogger())
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
                std::tuple(lhs.Priority, lhs.ReaderIndex, lhs.BlockIndex) <
                std::tuple(rhs.Priority, rhs.ReaderIndex, rhs.BlockIndex);
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

    MemoryManagerHolder_->Get()->SetTotalSize(totalBlockUncompressedSize + Config_->WindowSize);
    MemoryManagerHolder_->Get()->SetPrefetchMemorySize(std::min(Config_->WindowSize, totalRemainingSize));

    YT_LOG_DEBUG("Creating block fetcher (BlockDescriptors: %v)",
        MakeCompactIntervalView(blockDescriptors));

    YT_VERIFY(totalRemainingSize > 0);
}

TBlockFetcher::~TBlockFetcher()
{
    YT_UNUSED_FUTURE(MemoryManagerHolder_->Get()->Finalize());
}

void TBlockFetcher::Start()
{
    YT_VERIFY(!Started_.exchange(true));

    FetchNextGroupMemoryFuture_ =
        MemoryManagerHolder_->Get()->AsyncAcquire(
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
    // Note that the last call to OnReaderFinished_ happens-before the end of the last FetchBlock() / FetchBlocks(),
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

std::vector<TFuture<TBlock>> TBlockFetcher::FetchBlocks(const std::vector<TBlockDescriptor>& blockDescriptors)
{
    YT_LOG_DEBUG("Fetching blocks (BlockDescriptors: %v)",
        MakeCompactIntervalView(blockDescriptors));

    YT_VERIFY(Started_);
    YT_VERIFY(HasMoreBlocks());

    std::vector<std::pair<TBlockDescriptor, int>> blockDescriptorWithIndexList;
    blockDescriptorWithIndexList.reserve(blockDescriptors.size());
    for (int index = 0; index < std::ssize(blockDescriptors); ++index) {
        blockDescriptorWithIndexList.emplace_back(std::pair(blockDescriptors[index], index));
    }

    std::sort(blockDescriptorWithIndexList.begin(), blockDescriptorWithIndexList.end());

    std::vector<int> windowIndicesToRequest;
    std::vector<int> windowIndicesToRelease;

    std::vector<TFuture<TBlock>> blockFutures;
    blockFutures.resize(blockDescriptors.size());

    i64 outOfTurnBlockCount = 0;

    for (const auto& [blockDescriptor, originalIndex] : blockDescriptorWithIndexList) {
        auto [readerIndex, blockIndex] = blockDescriptor;
        int windowIndex = GetOrCrash(Chunks_[readerIndex].BlockIndexToWindowIndex, blockIndex);
        auto& windowSlot = Window_[windowIndex];

        auto blockPromise = GetBlockPromise(windowSlot);

        YT_VERIFY(windowSlot.RemainingFetches > 0);
        if (!windowSlot.FetchStarted.test_and_set()) {
            ++outOfTurnBlockCount;

            auto chunkId = Chunks_[readerIndex].Reader->GetChunkId();

            YT_LOG_DEBUG("Fetching block out of turn "
                "(ChunkId: %v, Block: %v, WindowIndex: %v)",
                chunkId,
                blockIndex,
                windowIndex);

            const auto& blockInfo = BlockInfos_[windowIndex];
            windowSlot.MemoryUsageGuard = MemoryManagerHolder_->Get()->Acquire(
                blockInfo.UncompressedDataSize);

            TBlockId blockId(chunkId, blockIndex);

            auto cachedBlock = Config_->UseUncompressedBlockCache
                ? BlockCache_->FindBlock(blockId, blockInfo.BlockType)
                : TCachedBlock();
            if (cachedBlock) {
                ChunkReadOptions_.ChunkReaderStatistics->DataBytesReadFromCache.fetch_add(
                    cachedBlock.Size(),
                    std::memory_order::relaxed);

                cachedBlock.Data = TrackMemory(ChunkReadOptions_.MemoryUsageTracker, std::move(cachedBlock.Data));

                TRef ref = cachedBlock.Data;
                windowSlot.MemoryUsageGuard->CaptureBlock(std::move(cachedBlock.Data));

                cachedBlock = TBlock(TSharedRef(ref, MakeSharedRangeHolder(std::move(windowSlot.MemoryUsageGuard))));

                DoStartBlock(blockInfo);
                DoSetBlock(blockInfo, windowSlot, std::move(cachedBlock));
            } else {
                windowIndicesToRequest.push_back(windowIndex);
            }
        }

        auto blockFuture = blockPromise.ToFuture();
        if (--windowSlot.RemainingFetches == 0 && blockFuture.IsSet()) {
            windowIndicesToRelease.push_back(windowIndex);
        }

        --TotalRemainingFetches_;

        blockFutures[originalIndex] = blockFuture;
    }

    ChunkReadOptions_.ChunkReaderStatistics->BlockCount.fetch_add(
        outOfTurnBlockCount,
        std::memory_order::relaxed);

    if (!windowIndicesToRelease.empty()) {
        ReaderInvoker_->Invoke(
            BIND(&TBlockFetcher::ReleaseBlocks,
                MakeWeak(this),
                std::move(windowIndicesToRelease)));
    }

    if (!windowIndicesToRequest.empty()) {
        std::vector<int> groupWindowIndices;
        std::vector<TBlockDescriptor> groupBlockDescriptors;
        i64 estimatedCompressedSize = 0;

        auto requestBlocks = [&] {
            YT_LOG_DEBUG("Requesting blocks async (BlockDescriptors: %v)",
                MakeCompactIntervalView(groupBlockDescriptors));

            ReaderInvoker_->Invoke(
                BIND(&TBlockFetcher::RequestBlocks,
                    MakeWeak(this),
                    std::move(groupWindowIndices),
                    std::move(groupBlockDescriptors)));

            estimatedCompressedSize = 0;
        };

        i64 index = 0;

        while (index < std::ssize(windowIndicesToRequest)) {
            auto windowIndex = windowIndicesToRequest[index];
            const auto& blockInfo = BlockInfos_[windowIndex];

            i64 estimatedCompressedBlockSize = blockInfo.UncompressedDataSize * CompressionRatio_;
            bool canGroupBlocks = Config_->GroupOutOfOrderBlocks
                && estimatedCompressedSize + estimatedCompressedBlockSize <= Config_->GroupSize;

            if (groupWindowIndices.empty() || canGroupBlocks) {
                groupWindowIndices.push_back(windowIndex);
                groupBlockDescriptors.push_back(TBlockDescriptor {
                    .ReaderIndex = blockInfo.ReaderIndex,
                    .BlockIndex = blockInfo.BlockIndex,
                });

                estimatedCompressedSize += estimatedCompressedBlockSize;
                ++index;
            } else {
                requestBlocks();
            }
        }

        if (!groupWindowIndices.empty()) {
            requestBlocks();
        }
    }

    return blockFutures;
}

std::vector<TFuture<TBlock>> TBlockFetcher::FetchBlocks(const std::vector<int>& blockIndices)
{
    YT_VERIFY(std::ssize(Chunks_) == 1);

    std::vector<TBlockDescriptor> blockDescriptors;
    blockDescriptors.reserve(blockIndices.size());

    for (auto blockIndex : blockIndices) {
        blockDescriptors.emplace_back(TBlockDescriptor{
            .ReaderIndex = 0,
            .BlockIndex = blockIndex,
        });
    }

    return FetchBlocks(std::move(blockDescriptors));
}

TFuture<TBlock> TBlockFetcher::FetchBlock(int readerIndex, int blockIndex)
{
    auto blockFutures = FetchBlocks(std::vector{TBlockDescriptor{
        .ReaderIndex = readerIndex,
        .BlockIndex = blockIndex,
    }});

    YT_VERIFY(std::ssize(blockFutures) == 1);

    return blockFutures[0];
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
                "(ChunkId: %v, Block: %v, WindowIndex: %v, Codec: %v)",
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
                "(ChunkId: %v, Block: %v, WindowIndex: %v, CompressedSize: %v, UncompressedSize: %v, Codec: %v)",
                chunkId,
                blockIndex,
                windowIndex,
                compressedBlock.Size(),
                uncompressedBlock.Size(),
                Codec_->GetId());
        }

        UncompressedDataSize_ += uncompressedBlock.Size();
        CompressedDataSize_ += compressedBlockSize;

        uncompressedBlock = TrackMemory(
            ChunkReadOptions_.MemoryUsageTracker,
            std::move(uncompressedBlock));

        if (Config_->UseUncompressedBlockCache) {
            BlockCache_->PutBlock(
                blockId,
                blockInfo.BlockType,
                TBlock(uncompressedBlock));
        }

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
    i64 prefetchedBlockCount = 0;

    while (FirstUnfetchedWindowIndex_ < std::ssize(BlockInfos_)) {
        const auto& blockInfo = BlockInfos_[FirstUnfetchedWindowIndex_];
        auto readerIndex = blockInfo.ReaderIndex;
        auto blockIndex = blockInfo.BlockIndex;
        auto chunkId = Chunks_[readerIndex].Reader->GetChunkId();

        if (windowIndexes.empty() || uncompressedSize + blockInfo.UncompressedDataSize <= availableSlots) {
            if (Window_[FirstUnfetchedWindowIndex_].FetchStarted.test_and_set()) {
                // This block has been already requested out of order.
                YT_LOG_DEBUG("Skipping out of turn block (ChunkId: %v, Block: %v, WindowIndex: %v)",
                    chunkId,
                    blockIndex,
                    FirstUnfetchedWindowIndex_);
                ++FirstUnfetchedWindowIndex_;
                continue;
            }

            ++prefetchedBlockCount;

            auto memoryToTransfer = std::min(
                static_cast<i64>(blockInfo.UncompressedDataSize),
                underlyingGuard->GetSlots());
            Window_[FirstUnfetchedWindowIndex_].MemoryUsageGuard = memoryUsageGuard->TransferMemory(memoryToTransfer);

            TBlockId blockId(chunkId, blockIndex);
            auto cachedBlock = Config_->UseUncompressedBlockCache
                ? BlockCache_->FindBlock(blockId, blockInfo.BlockType)
                : TCachedBlock();
            if (cachedBlock) {
                ChunkReadOptions_.ChunkReaderStatistics->DataBytesReadFromCache.fetch_add(
                    cachedBlock.Size(),
                    std::memory_order::relaxed);

                cachedBlock.Data = TrackMemory(ChunkReadOptions_.MemoryUsageTracker, std::move(cachedBlock.Data));

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

    ChunkReadOptions_.ChunkReaderStatistics->BlockCount.fetch_add(
        prefetchedBlockCount,
        std::memory_order::relaxed);

    ChunkReadOptions_.ChunkReaderStatistics->PrefetchedBlockCount.fetch_add(
        prefetchedBlockCount,
        std::memory_order::relaxed);

    if (windowIndexes.empty()) {
        FetchingCompleted_ = true;
        YT_UNUSED_FUTURE(MemoryManagerHolder_->Get()->Finalize());
        return;
    }

    if (TotalRemainingSize_ > 0) {
        auto nextGroupSize = std::min<i64>(TotalRemainingSize_, Config_->GroupSize);
        MemoryManagerHolder_->Get()->SetPrefetchMemorySize(nextGroupSize);
        FetchNextGroupMemoryFuture_ = MemoryManagerHolder_->Get()->AsyncAcquire(nextGroupSize);
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
        ::NYT::MakeCompactIntervalView(windowIndexes));

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
            ::NYT::MakeCompactIntervalView(blockIndices),
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
        ::NYT::MakeCompactIntervalView(blockIndexes));

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

auto TBlockFetcher::TValueGetter::operator()(const TIterator& iterator) const
    -> TValue
{
    return iterator->BlockIndex;
}

TBlockFetcher::TIntervalFormatter::TIntervalFormatter(const TBlockFetcher* blockFetcher)
    : BlockFetcher(blockFetcher)
{
    YT_VERIFY(blockFetcher);
}

void TBlockFetcher::TIntervalFormatter::operator()(
    TStringBuilderBase* builder,
    const TIterator& first,
    const TIterator& last,
    const TValueGetter& valueGetter,
    bool firstInterval) const
{
    auto readerIndex = first->ReaderIndex;
    bool appendChunkId = false;

    if (!firstInterval) {
        if (std::prev(first)->ReaderIndex != readerIndex) {
            builder->AppendString("; ");
            appendChunkId = true;
        } else {
            builder->AppendString(", ");
        }
    } else {
        appendChunkId = true;
    }

    if (appendChunkId) {
        auto chunkId = BlockFetcher->Chunks_[readerIndex].Reader->GetChunkId();
        builder->AppendFormat("%v:", chunkId);
    }

    if (first == last) {
        builder->AppendFormat("%v", valueGetter(first));
    } else {
        builder->AppendFormat("%v-%v", valueGetter(first), valueGetter(last));
    }
}

auto TBlockFetcher::MakeCompactIntervalView(const TBlockDescriptors& blockDescriptors) const
    -> TCompactIntervalView<TBlockDescriptors, TValueGetter, TIntervalFormatter>
{
    return ::NYT::MakeCompactIntervalView(
        blockDescriptors,
        TValueGetter{},
        TIntervalFormatter{this});
}

////////////////////////////////////////////////////////////////////////////////

TSequentialBlockFetcher::TSequentialBlockFetcher(
    TBlockFetcherConfigPtr config,
    std::vector<TBlockInfo> blockInfos,
    TChunkReaderMemoryManagerHolderPtr memoryManagerHolder,
    std::vector<IChunkReaderPtr> chunkReaders,
    IBlockCachePtr blockCache,
    NCompression::ECodec codecId,
    double compressionRatio,
    const TClientChunkReadOptions& chunkReadOptions,
    IInvokerPtr sessionInvoker)
    : TBlockFetcher(
        std::move(config),
        blockInfos,
        std::move(memoryManagerHolder),
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
