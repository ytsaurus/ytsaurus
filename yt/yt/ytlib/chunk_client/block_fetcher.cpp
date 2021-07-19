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

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TBlockFetcher::TBlockFetcher(
    TBlockFetcherConfigPtr config,
    std::vector<TBlockInfo> blockInfos,
    TChunkReaderMemoryManagerPtr memoryManager,
    IChunkReaderPtr chunkReader,
    IBlockCachePtr blockCache,
    NCompression::ECodec codecId,
    double compressionRatio,
    const TClientChunkReadOptions& chunkReadOptions)
    : Config_(std::move(config))
    , BlockInfos_(std::move(blockInfos))
    , ChunkReader_(std::move(chunkReader))
    , BlockCache_(std::move(blockCache))
    , CompressionInvoker_(
        codecId == NCompression::ECodec::None
        ? nullptr
        : GetCompressionInvoker(chunkReadOptions.WorkloadDescriptor))
    , ReaderInvoker_(CreateSerializedInvoker(TDispatcher::Get()->GetReaderInvoker()))
    , CompressionRatio_(compressionRatio)
    , MemoryManager_(std::move(memoryManager))
    , Codec_(NCompression::GetCodec(codecId))
    , ChunkReadOptions_(chunkReadOptions)
    , Logger(ChunkClientLogger)
{
    YT_VERIFY(ChunkReader_);
    YT_VERIFY(BlockCache_);
    YT_VERIFY(!BlockInfos_.empty());

    Logger.AddTag("ChunkId: %v", ChunkReader_->GetChunkId());
    if (ChunkReadOptions_.ReadSessionId) {
        Logger.AddTag("ReadSessionId: %v", ChunkReadOptions_.ReadSessionId);
    }

    std::sort(
        BlockInfos_.begin(),
        BlockInfos_.end(),
        [] (const TBlockInfo& lhs, const TBlockInfo& rhs) {
            if (lhs.Priority != rhs.Priority) {
                return lhs.Priority < rhs.Priority;
            } else {
                return lhs.Index < rhs.Index;
            }
        });

    int windowSize = 1;
    i64 totalRemainingSize = 0;
    for (int index = 0; index + 1 < std::ssize(BlockInfos_); ++index) {
        if (BlockInfos_[index].Index != BlockInfos_[index + 1].Index) {
            ++windowSize;
        }
        totalRemainingSize += BlockInfos_[index].UncompressedDataSize;
    }
    totalRemainingSize += BlockInfos_.back().UncompressedDataSize;

    Window_ = std::make_unique<TWindowSlot[]>(windowSize);
    TotalRemainingFetches_ = BlockInfos_.size();
    TotalRemainingSize_ = totalRemainingSize;

    // We consider contiguous segments consisting of the same block and store them
    // in the BlockIndexToWindowIndex hashmap.
    // [leftIndex, rightIndex) is a half-interval containing all blocks
    // equal to BlockInfos[leftIndex].
    // We also explicitly unique the elements of BlockInfos_.
    std::vector<int> blockIndexes;
    blockIndexes.reserve(windowSize);
    i64 totalBlockUncompressedSize = 0;
    int windowIndex = 0;
    for (int leftIndex = 0, rightIndex = 0; leftIndex != std::ssize(BlockInfos_); leftIndex = rightIndex) {
        auto& currentBlock = BlockInfos_[leftIndex];
        while (rightIndex != std::ssize(BlockInfos_) && BlockInfos_[rightIndex].Index == currentBlock.Index) {
            ++rightIndex;
        }

        // Contrary would mean that the same block was requested twice with different priorities.
        YT_VERIFY(!BlockIndexToWindowIndex_.contains(currentBlock.Index));

        BlockIndexToWindowIndex_[currentBlock.Index] = windowIndex;
        Window_[windowIndex].RemainingFetches = rightIndex - leftIndex;
        blockIndexes.push_back(currentBlock.Index);
        totalBlockUncompressedSize += currentBlock.UncompressedDataSize;
        if (windowIndex != leftIndex) {
            BlockInfos_[windowIndex] = std::move(currentBlock); // Similar to std::unique.
        }
        ++windowIndex;
    }
    YT_VERIFY(windowIndex == windowSize);

    // Now Window_ and BlockInfos_ correspond to each other.
    BlockInfos_.resize(windowSize);

    MemoryManager_->SetTotalSize(totalBlockUncompressedSize + Config_->WindowSize);
    MemoryManager_->SetPrefetchMemorySize(std::min(Config_->WindowSize, totalRemainingSize));

    YT_LOG_DEBUG("Creating block fetcher (Blocks: %v)",
        blockIndexes);

    YT_VERIFY(totalRemainingSize > 0);

    FetchNextGroupMemoryFuture_ =
        MemoryManager_->AsyncAcquire(
            std::min(totalRemainingSize, Config_->GroupSize));
    FetchNextGroupMemoryFuture_.Subscribe(BIND(
        &TBlockFetcher::FetchNextGroup,
            MakeWeak(this))
        .Via(ReaderInvoker_));
}

TBlockFetcher::~TBlockFetcher()
{
    MemoryManager_->Finalize();
}

bool TBlockFetcher::HasMoreBlocks() const
{
    return TotalRemainingFetches_ > 0;
}

i64 TBlockFetcher::GetBlockSize(int blockIndex) const
{
    int windowIndex = GetOrCrash(BlockIndexToWindowIndex_, blockIndex);
    return BlockInfos_[windowIndex].UncompressedDataSize;
}

TFuture<TBlock> TBlockFetcher::FetchBlock(int blockIndex)
{
    YT_VERIFY(HasMoreBlocks());

    int windowIndex = GetOrCrash(BlockIndexToWindowIndex_, blockIndex);
    auto& windowSlot = Window_[windowIndex];

    auto blockPromise = GetBlockPromise(windowSlot);

    YT_VERIFY(windowSlot.RemainingFetches > 0);
    if (!windowSlot.FetchStarted.test_and_set()) {
        YT_LOG_DEBUG("Fetching block out of turn (BlockIndex: %v, WindowIndex: %v)",
            blockIndex,
            windowIndex);

        windowSlot.MemoryUsageGuard = MemoryManager_->Acquire(
            BlockInfos_[windowIndex].UncompressedDataSize);

        TBlockId blockId(ChunkReader_->GetChunkId(), blockIndex);
        if (auto uncompressedBlock = BlockCache_->FindBlock(blockId, EBlockType::UncompressedData).Block) {
            ChunkReadOptions_.ChunkReaderStatistics->DataBytesReadFromCache += uncompressedBlock.Size();

            TRef ref = uncompressedBlock.Data;
            windowSlot.MemoryUsageGuard->CaptureBlock(std::move(uncompressedBlock.Data));
            blockPromise.Set(TBlock(TSharedRef(ref, std::move(windowSlot.MemoryUsageGuard))));
            TotalRemainingSize_ -= BlockInfos_[windowIndex].UncompressedDataSize;
        } else {
            ReaderInvoker_->Invoke(BIND(
                &TBlockFetcher::RequestBlocks,
                MakeWeak(this),
                std::vector{windowIndex},
                std::vector{blockIndex},
                BlockInfos_[windowIndex].UncompressedDataSize));
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
        int blockIndex = blockInfo.Index;

        TBlockId blockId(ChunkReader_->GetChunkId(), blockInfo.Index);

        TSharedRef uncompressedBlock;
        if (Codec_->GetId() == NCompression::ECodec::None) {
            uncompressedBlock = std::move(compressedBlock.Data);
        } else {
            YT_LOG_DEBUG("Started decompressing block (BlockIndex: %v, WindowIndex: %v, Codec: %v)",
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
                "(BlockIndex: %v, WindowIndex: %v, CompressedSize: %v, UncompressedSize: %v, Codec: %v)",
                blockIndex,
                windowIndex,
                compressedBlock.Size(),
                uncompressedBlock.Size(),
                Codec_->GetId());
        }

        auto& windowSlot = Window_[windowIndex];
        TRef ref = uncompressedBlock;
        windowSlot.MemoryUsageGuard->CaptureBlock(uncompressedBlock);
        GetBlockPromise(windowSlot).Set(TBlock(TSharedRef(
            ref,
            std::move(windowSlot.MemoryUsageGuard))));
        if (windowSlot.RemainingFetches == 0) {
            windowIndexesToRelease.push_back(windowIndex);
        }

        UncompressedDataSize_ += uncompressedBlock.Size();
        CompressedDataSize_ += compressedBlockSize;

        BlockCache_->PutBlock(
            blockId,
            EBlockType::UncompressedData,
            TBlock(std::move(uncompressedBlock)));
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
    std::vector<int> blockIndexes;
    i64 uncompressedSize = 0;
    i64 availableSlots = underlyingGuard->GetSlots();
    while (FirstUnfetchedWindowIndex_ < std::ssize(BlockInfos_)) {
        const auto& blockInfo = BlockInfos_[FirstUnfetchedWindowIndex_];
        int blockIndex = blockInfo.Index;
        if (windowIndexes.empty() || uncompressedSize + blockInfo.UncompressedDataSize <= availableSlots) {
            if (Window_[FirstUnfetchedWindowIndex_].FetchStarted.test_and_set()) {
                // This block has been already requested out of order.
                YT_LOG_DEBUG("Skipping out of turn block (BlockIndex: %v, WindowIndex: %v)",
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

            TBlockId blockId(ChunkReader_->GetChunkId(), blockIndex);
            if (auto uncompressedBlock = BlockCache_->FindBlock(blockId, EBlockType::UncompressedData).Block) {
                ChunkReadOptions_.ChunkReaderStatistics->DataBytesReadFromCache += uncompressedBlock.Size();

                auto& windowSlot = Window_[FirstUnfetchedWindowIndex_];
                TRef ref = uncompressedBlock.Data;
                windowSlot.MemoryUsageGuard->CaptureBlock(std::move(uncompressedBlock.Data));
                GetBlockPromise(windowSlot).Set(TBlock(TSharedRef(
                    ref,
                    std::move(windowSlot.MemoryUsageGuard))));
                TotalRemainingSize_ -= blockInfo.UncompressedDataSize;
            } else {
                uncompressedSize += blockInfo.UncompressedDataSize;
                windowIndexes.push_back(FirstUnfetchedWindowIndex_);
                blockIndexes.push_back(blockIndex);
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
        std::move(blockIndexes),
        uncompressedSize);
}

void TBlockFetcher::MarkFailedBlocks(const std::vector<int>& windowIndexes, const TError& error)
{
    for (auto index : windowIndexes) {
        GetBlockPromise(Window_[index]).Set(error);
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
    std::vector<int> blockIndexes,
    i64 uncompressedSize)
{
    YT_LOG_DEBUG("Requesting block group (Blocks: %v, UncompressedSize: %v)",
        MakeShrunkFormattableView(blockIndexes, TDefaultFormatter(), 3),
        uncompressedSize);

    TotalRemainingSize_ -= uncompressedSize;

    auto future = ChunkReader_->ReadBlocks(
        ChunkReadOptions_,
        blockIndexes,
        static_cast<i64>(uncompressedSize * CompressionRatio_));

    // NB: Handling |OnGotBlocks| in an arbitrary thread seems OK.
    future.SubscribeUnique(
        BIND(
            &TBlockFetcher::OnGotBlocks,
            MakeWeak(this),
            Passed(std::move(windowIndexes)),
            Passed(std::move(blockIndexes))));
}

void TBlockFetcher::OnGotBlocks(
    std::vector<int> windowIndexes,
    std::vector<int> blockIndexes,
    TErrorOr<std::vector<TBlock>>&& blocksOrError)
{
    if (!blocksOrError.IsOK()) {
        MarkFailedBlocks(windowIndexes, blocksOrError);
        return;
    }

    YT_LOG_DEBUG("Got block group (Blocks: %v)",
        MakeShrunkFormattableView(blockIndexes, TDefaultFormatter(), 3));

    if (Codec_->GetId() == NCompression::ECodec::None) {
        DecompressBlocks(
            std::move(windowIndexes),
            std::move(blocksOrError.Value()));
    } else {
        CompressionInvoker_->Invoke(BIND(
            &TBlockFetcher::DecompressBlocks,
            MakeWeak(this),
            Passed(std::move(windowIndexes)),
            Passed(std::move(blocksOrError.Value()))));
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
    IChunkReaderPtr chunkReader,
    IBlockCachePtr blockCache,
    NCompression::ECodec codecId,
    double compressionRatio,
    const TClientChunkReadOptions& chunkReadOptions)
    : TBlockFetcher(
        std::move(config),
        blockInfos,
        std::move(memoryManager),
        std::move(chunkReader),
        std::move(blockCache),
        codecId,
        compressionRatio,
        chunkReadOptions)
    , OriginalOrderBlockInfos_(std::move(blockInfos))
{ }

TFuture<TBlock> TSequentialBlockFetcher::FetchNextBlock()
{
    YT_VERIFY(CurrentIndex_ < std::ssize(OriginalOrderBlockInfos_));
    return FetchBlock(OriginalOrderBlockInfos_[CurrentIndex_++].Index);
}

i64 TSequentialBlockFetcher::GetNextBlockSize() const
{
    YT_VERIFY(CurrentIndex_ < std::ssize(OriginalOrderBlockInfos_));
    return OriginalOrderBlockInfos_[CurrentIndex_].UncompressedDataSize;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
