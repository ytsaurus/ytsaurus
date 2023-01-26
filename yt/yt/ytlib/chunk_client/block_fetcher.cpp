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
    , CompressionInvoker_(SessionInvoker_ ? SessionInvoker_ : GetCompressionInvoker(chunkReadOptions.WorkloadDescriptor))
    , ReaderInvoker_(CreateSerializedInvoker(SessionInvoker_ ? SessionInvoker_ : TDispatcher::Get()->GetReaderInvoker()))
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
        Logger.AddTag("ReadSessionId: %v", ChunkReadOptions_.ReadSessionId);
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
    return BIND(
        &TBlockFetcher::DoFetchBlock,
        MakeStrong(this),
        readerIndex,
        blockIndex)
        .AsyncVia(ReaderInvoker_)
        .Run();
}

TFuture<TBlock> TBlockFetcher::DoFetchBlock(int readerIndex, int blockIndex)
{
    VERIFY_SERIALIZED_INVOKER_AFFINITY(ReaderInvoker_);
    YT_VERIFY(Started_);
    YT_VERIFY(HasMoreBlocks());

    TForbidContextSwitchGuard guard;

    int windowIndex = GetOrCrash(Chunks_[readerIndex].BlockIndexToWindowIndex, blockIndex);
    auto& windowSlot = Window_[windowIndex];

    YT_VERIFY(windowSlot.RemainingFetches > 0);
    if (!std::exchange(windowSlot.FetchStarted, true)) {
        auto chunkId = Chunks_[readerIndex].Reader->GetChunkId();

        YT_LOG_DEBUG("Fetching block out of turn "
            "(ChunkId: %v, BlockIndex: %v, WindowIndex: %v)",
            chunkId,
            blockIndex,
            windowIndex);

        windowSlot.MemoryUsageGuard = MemoryManager_->Acquire(
            BlockInfos_[windowIndex].UncompressedDataSize);

        TBlockId blockId(chunkId, blockIndex);

        TFuture<std::vector<TBlock>> asyncBlocks;

        auto cachedBlock = Config_->UseUncompressedBlockCache
            ? BlockCache_->FindBlock(blockId, EBlockType::UncompressedData).Block
            : TBlock();
        if (cachedBlock) {
            ChunkReadOptions_.ChunkReaderStatistics->DataBytesReadFromCache.fetch_add(
                cachedBlock.Size(),
                std::memory_order::relaxed);

            cachedBlock.Data = TrackMemory(ChunkReadOptions_.MemoryReferenceTracker, std::move(cachedBlock.Data));

            TRef ref = cachedBlock.Data;
            windowSlot.MemoryUsageGuard->CaptureBlock(std::move(cachedBlock.Data));

            cachedBlock = TBlock(TSharedRef(ref, MakeSharedRangeHolder(std::move(windowSlot.MemoryUsageGuard))));

            DoStartBlock(BlockInfos_[windowIndex]);
            asyncBlocks = MakeFuture<std::vector<TBlock>>(std::vector{std::move(cachedBlock)});
        } else {
            TBlockDescriptor blockDescriptor{
                .ReaderIndex = readerIndex,
                .BlockIndex = blockIndex
            };
            asyncBlocks = BIND(
                &TBlockFetcher::RequestBlocks,
                MakeStrong(this),
                std::vector{windowIndex},
                std::vector{blockDescriptor})
                .AsyncVia(ReaderInvoker_)
                .Run();
        }

        SetAsyncBlocks(std::vector{windowIndex}, std::move(asyncBlocks));
    }

    auto asyncBlock = windowSlot.AsyncBlock;
    // No one else needs this block, so it can be removed from block fetcher.
    if (--windowSlot.RemainingFetches == 0) {
        ReaderInvoker_->Invoke(
            BIND(&TBlockFetcher::ReleaseBlocks,
                MakeWeak(this),
                std::vector{windowIndex}));
    }

    --TotalRemainingFetches_;

    return asyncBlock;
}

TFuture<TBlock> TBlockFetcher::FetchBlock(int blockIndex)
{
    YT_VERIFY(std::ssize(Chunks_) == 1);
    return FetchBlock(/*readerIndex*/ 0, blockIndex);
}

std::vector<TBlock> TBlockFetcher::MaybeDecompressAndCacheBlocks(
    std::vector<int> windowIndices,
    std::vector<TBlock> compressedBlocks)
{
    YT_VERIFY(windowIndices.size() == compressedBlocks.size());

    std::vector<TBlock> blocks;
    blocks.reserve(compressedBlocks.size());

    for (int index = 0; index < std::ssize(compressedBlocks); ++index) {
        auto& compressedBlock = compressedBlocks[index];
        auto compressedBlockSize = compressedBlock.Size();
        int windowIndex = windowIndices[index];
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
                EBlockType::UncompressedData,
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

        blocks.push_back(TBlock(std::move(uncompressedBlock)));
    }

    return blocks;
}

void TBlockFetcher::FetchNextGroup(const TErrorOr<TMemoryUsageGuardPtr>& memoryUsageGuardOrError)
{
    VERIFY_SERIALIZED_INVOKER_AFFINITY(ReaderInvoker_);

    TForbidContextSwitchGuard guard;

    if (!memoryUsageGuardOrError.IsOK()) {
        YT_LOG_INFO(memoryUsageGuardOrError,
            "Failed to acquire memory in chunk reader memory manager");
        return;
    }

    const auto& memoryUsageGuard = memoryUsageGuardOrError.Value();
    auto* underlyingGuard = memoryUsageGuard->GetGuard();

    std::vector<int> windowIndices;
    std::vector<TBlockDescriptor> blockDescriptors;
    i64 uncompressedSize = 0;
    i64 availableSlots = underlyingGuard->GetSlots();

    while (FirstUnfetchedWindowIndex_ < std::ssize(BlockInfos_)) {
        const auto& blockInfo = BlockInfos_[FirstUnfetchedWindowIndex_];
        auto readerIndex = blockInfo.ReaderIndex;
        auto blockIndex = blockInfo.BlockIndex;
        auto chunkId = Chunks_[readerIndex].Reader->GetChunkId();

        if (windowIndices.empty() || uncompressedSize + blockInfo.UncompressedDataSize <= availableSlots) {
            auto& windowSlot = Window_[FirstUnfetchedWindowIndex_];
            if (std::exchange(windowSlot.FetchStarted, true)) {
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
            windowSlot.MemoryUsageGuard = New<TMemoryUsageGuard>(
                std::move(transferred),
                memoryUsageGuard->GetMemoryManager());

            TBlockId blockId(chunkId, blockIndex);
            auto cachedBlock = Config_->UseUncompressedBlockCache
                ? BlockCache_->FindBlock(blockId, EBlockType::UncompressedData).Block
                : TBlock();
            if (cachedBlock) {
                ChunkReadOptions_.ChunkReaderStatistics->DataBytesReadFromCache.fetch_add(
                    cachedBlock.Size(),
                    std::memory_order::relaxed);

                cachedBlock.Data = TrackMemory(ChunkReadOptions_.MemoryReferenceTracker, std::move(cachedBlock.Data));

                TRef ref = cachedBlock.Data;
                windowSlot.MemoryUsageGuard->CaptureBlock(std::move(cachedBlock.Data));
                cachedBlock = TBlock(TSharedRef(
                    ref,
                    MakeSharedRangeHolder(std::move(windowSlot.MemoryUsageGuard))));

                DoStartBlock(blockInfo);
                auto asyncBlocks = MakeFuture<std::vector<TBlock>>(std::vector{std::move(cachedBlock)});
                SetAsyncBlocks(
                    std::vector{FirstUnfetchedWindowIndex_},
                    std::move(asyncBlocks));
            } else {
                uncompressedSize += blockInfo.UncompressedDataSize;
                windowIndices.push_back(FirstUnfetchedWindowIndex_);
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

    if (windowIndices.empty()) {
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

    auto asyncBlocks = RequestBlocks(
        windowIndices,
        std::move(blockDescriptors));
    SetAsyncBlocks(std::move(windowIndices), std::move(asyncBlocks));
}

void TBlockFetcher::ReleaseBlocks(const std::vector<int>& windowIndices)
{
    VERIFY_SERIALIZED_INVOKER_AFFINITY(ReaderInvoker_);

    YT_LOG_DEBUG("Releasing blocks (WindowIndices: %v)",
        MakeShrunkFormattableView(windowIndices, TDefaultFormatter(), 3));

    for (auto index : windowIndices) {
        Window_[index].AsyncBlock.Reset();
    }
}

TFuture<std::vector<TBlock>> TBlockFetcher::RequestBlocks(
    std::vector<int> windowIndices,
    std::vector<TBlockDescriptor> blockDescriptors)
{
    YT_VERIFY(windowIndices.size() == blockDescriptors.size());

    // Position of window index in |windowIndices|.
    THashMap<int, int> windowIndexToPosition;
    for (int index = 0; index < std::ssize(windowIndices); ++index) {
        EmplaceOrCrash(windowIndexToPosition, windowIndices[index], index);
    }

    std::vector<TFuture<TBlock>> result;
    result.resize(windowIndices.size());

    i64 uncompressedSize = 0;
    for (auto index : windowIndices) {
        DoStartBlock(BlockInfos_[index]);
        uncompressedSize += BlockInfos_[index].UncompressedDataSize;
    }

    THashMap<int, std::vector<int>> readerIndexToBlockIndices;
    THashMap<int, std::vector<int>> readerIndexToWindowIndices;
    for (int index = 0; index < std::ssize(blockDescriptors); ++index) {
        const auto& blockDescriptor = blockDescriptors[index];
        auto readerIndex = blockDescriptor.ReaderIndex;
        readerIndexToBlockIndices[readerIndex].push_back(blockDescriptor.BlockIndex);
        readerIndexToWindowIndices[readerIndex].push_back(windowIndices[index]);
    }

    for (auto& indexPair : readerIndexToBlockIndices) {
        auto readerIndex = indexPair.first;
        auto blockIndices = std::move(indexPair.second);
        const auto& chunkReader = Chunks_[readerIndex].Reader;

        YT_LOG_DEBUG("Requesting block group (ChunkId: %v, Blocks: %v, UncompressedSize: %v)",
            chunkReader->GetChunkId(),
            MakeShrunkFormattableView(blockIndices, TDefaultFormatter(), 3),
            uncompressedSize);

        auto asyncRawBlocks = [&] {
            std::optional<TTraceContextGuard> maybeGuard;
            if (Chunks_[readerIndex].TraceContext.has_value()) {
                maybeGuard.emplace(*Chunks_[readerIndex].TraceContext);
            }

            return chunkReader->ReadBlocks(
                ChunkReadOptions_,
                blockIndices,
                static_cast<i64>(uncompressedSize * CompressionRatio_),
                SessionInvoker_);
        }();

        const auto& windowIndices = readerIndexToWindowIndices[readerIndex];
        YT_VERIFY(windowIndices.size() == blockIndices.size());
        // NB: Handling |MaybeDecompressBlocks| in an arbitrary thread seems OK.
        auto asyncBlocks = asyncRawBlocks
            .ApplyUnique(BIND(
                &TBlockFetcher::MaybeDecompressBlocks,
                MakeStrong(this),
                readerIndex,
                windowIndices,
                Passed(std::move(blockIndices))));
        // Fast path.
        if (std::ssize(readerIndexToBlockIndices) == 1) {
            return asyncBlocks;
        }

        for (int index = 0; index < std::ssize(windowIndices); ++index) {
            auto windowIndex = windowIndices[index];
            auto position = GetOrCrash(windowIndexToPosition, windowIndex);
            auto asyncBlock = asyncBlocks.Apply(BIND(
                [=] (const std::vector<TBlock>& blocks) {
                    return blocks[index];
                }));
            result[position] = std::move(asyncBlock);
        }
    }

    return AllSucceeded(std::move(result));
}

TFuture<std::vector<TBlock>> TBlockFetcher::MaybeDecompressBlocks(
    int readerIndex,
    std::vector<int> windowIndices,
    std::vector<int> blockIndices,
    TErrorOr<std::vector<TBlock>>&& blocksOrError)
{
    if (!blocksOrError.IsOK()) {
        return MakeFuture<std::vector<TBlock>>(blocksOrError);
    }

    auto blocks = std::move(blocksOrError.Value());

    YT_VERIFY(blocks.size() == blockIndices.size());
    YT_VERIFY(blocks.size() == windowIndices.size());

    auto chunkId = Chunks_[readerIndex].Reader->GetChunkId();
    YT_LOG_DEBUG("Got block group (ChunkId: %v, Blocks: %v)",
        chunkId,
        MakeShrunkFormattableView(blockIndices, TDefaultFormatter(), 3));

    if (Codec_->GetId() == NCompression::ECodec::None) {
        auto processedBlocks = MaybeDecompressAndCacheBlocks(
            std::move(windowIndices),
            std::move(blocks));
        return MakeFuture<std::vector<TBlock>>(std::move(processedBlocks));
    } else {
        return BIND(
            &TBlockFetcher::MaybeDecompressAndCacheBlocks,
            MakeStrong(this),
            Passed(std::move(windowIndices)),
            Passed(std::move(blocks)))
            .AsyncVia(CompressionInvoker_)
            .Run();
    }
}

void TBlockFetcher::SetAsyncBlocks(
    std::vector<int> windowIndices,
    TFuture<std::vector<TBlock>> asyncBlocks)
{
    VERIFY_SERIALIZED_INVOKER_AFFINITY(ReaderInvoker_);

    for (int index = 0; index < std::ssize(windowIndices); ++index) {
        auto windowIndex = windowIndices[index];
        auto windowIndicesSize = windowIndices.size();

        auto asyncBlock = asyncBlocks.Apply(
            BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TBlock>>& blocksOrError) {
                DoFinishBlock(BlockInfos_[windowIndex]);

                const auto& blocks = blocksOrError
                    .ValueOrThrow();
                YT_VERIFY(blocks.size() == windowIndicesSize);
                return std::move(blocks[index]);
            }).AsyncVia(ReaderInvoker_));
        Window_[windowIndex].AsyncBlock = std::move(asyncBlock);
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
        NProfiling::ValueToDuration(DecompressionTime_),
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
    IInvokerPtr sessionnInvoker)
    : TBlockFetcher(
        std::move(config),
        blockInfos,
        std::move(memoryManager),
        std::move(chunkReaders),
        std::move(blockCache),
        codecId,
        compressionRatio,
        chunkReadOptions,
        std::move(sessionnInvoker))
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
