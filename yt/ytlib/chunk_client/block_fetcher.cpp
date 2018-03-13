#include "block_fetcher.h"
#include "private.h"
#include "block_cache.h"
#include "config.h"
#include "dispatcher.h"

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/core/compression/codec.h>

#include <yt/core/concurrency/action_queue.h>

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TBlockFetcher::TBlockFetcher(
    TBlockFetcherConfigPtr config,
    std::vector<TBlockInfo> blockInfos,
    TAsyncSemaphorePtr asyncSemaphore,
    IChunkReaderPtr chunkReader,
    IBlockCachePtr blockCache,
    NCompression::ECodec codecId,
    const TReadSessionId& sessionId)
    : Config_(std::move(config))
    , BlockInfos_(std::move(blockInfos))
    , ChunkReader_(std::move(chunkReader))
    , BlockCache_(std::move(blockCache))
    , CompressionInvoker_(CreateFixedPriorityInvoker(
        TDispatcher::Get()->GetPrioritizedCompressionPoolInvoker(),
        Config_->WorkloadDescriptor.GetPriority()))
    , AsyncSemaphore_(std::move(asyncSemaphore))
    , Codec_(NCompression::GetCodec(codecId))
    , ReadSessionId_(sessionId)
    , Logger(ChunkClientLogger)
{
    YCHECK(ChunkReader_);
    YCHECK(BlockCache_);
    YCHECK(!BlockInfos_.empty());

    Logger.AddTag("ChunkId: %v", ChunkReader_->GetChunkId());
    if (ReadSessionId_) {
        Logger.AddTag("ReadSessionId: %v", ReadSessionId_);
    }

    std::sort(
        BlockInfos_.begin(),
        BlockInfos_.end(),
        [] (const TBlockInfo& lhs, const TBlockInfo& rhs) -> bool
        {
            if (lhs.Priority != rhs.Priority) {
                return lhs.Priority < rhs.Priority;
            } else {
                return lhs.Index < rhs.Index;
            }
        });

    std::vector<int> blockIndexes;

    WindowSize_ = 1;
    for (int index = 0; index + 1 < static_cast<int>(BlockInfos_.size()); ++index) {
        if (BlockInfos_[index].Index != BlockInfos_[index + 1].Index) {
            ++WindowSize_;
        }
    }

    Window_ = std::make_unique<TWindowSlot[]>(WindowSize_);
    TotalRemainingFetches_ = BlockInfos_.size();
    TotalRemainingSize_ = 0;

    for (int index = 0; index < static_cast<int>(BlockInfos_.size()); ++index) {
        TotalRemainingSize_ += BlockInfos_[index].UncompressedDataSize;
    }

    // We consider contiguous segments consisting of the same block and store them
    // in the BlockIndexToWindowIndex hashmap.
    // [leftIndex, rightIndex) is a half-interval containing all blocks
    // equal to BlockInfos[leftIndex].
    // We also explicitly unique the elements of BlockInfos_.
    for (int leftIndex = 0, rightIndex = 0; leftIndex != BlockInfos_.size(); leftIndex = rightIndex) {
        const auto& currentBlock = BlockInfos_[leftIndex];
        while (rightIndex != BlockInfos_.size() && BlockInfos_[rightIndex].Index == currentBlock.Index) {
            ++rightIndex;
        }

        // Contrary would mean that the same block was requested twice with different priorities.
        YCHECK(BlockIndexToWindowIndex_.find(currentBlock.Index) == BlockIndexToWindowIndex_.end());

        int windowIndex = BlockIndexToWindowIndex_.size();
        BlockIndexToWindowIndex_[currentBlock.Index] = windowIndex;
        Window_[windowIndex].RemainingFetches = rightIndex - leftIndex;
        BlockInfos_[windowIndex] = BlockInfos_[leftIndex]; // Similar to std::unique
        blockIndexes.push_back(currentBlock.Index);
    }

    // Now Window_ and BlockInfos_ correspond to each other.
    BlockInfos_.resize(WindowSize_);

    LOG_DEBUG("Creating block fetcher (Blocks: %v)",
        blockIndexes);

    YCHECK(TotalRemainingSize_ > 0);

    AsyncSemaphore_->AsyncAcquire(
        BIND(&TBlockFetcher::FetchNextGroup, MakeWeak(this)),
        TDispatcher::Get()->GetReaderInvoker(),
        std::min(static_cast<i64>(TotalRemainingSize_), Config_->GroupSize));
}

bool TBlockFetcher::HasMoreBlocks() const
{
    return TotalRemainingFetches_ > 0;
}

TFuture<TBlock> TBlockFetcher::FetchBlock(int blockIndex)
{
    YCHECK(HasMoreBlocks());

    auto iterator = BlockIndexToWindowIndex_.find(blockIndex);
    YCHECK(iterator != BlockIndexToWindowIndex_.end());
    int windowIndex = iterator->second;
    auto& windowSlot = Window_[windowIndex];

    YCHECK(windowSlot.RemainingFetches > 0);
    if (!windowSlot.FetchStarted.test_and_set()) {
        LOG_DEBUG("Fetching block out of turn (BlockIndex: %v, WindowIndex: %v)",
            blockIndex,
            windowIndex);

        windowSlot.AsyncSemaphoreGuard = std::make_unique<TAsyncSemaphoreGuard>(
            TAsyncSemaphoreGuard::Acquire(
                AsyncSemaphore_,
                BlockInfos_[windowIndex].UncompressedDataSize));

        TBlockId blockId(ChunkReader_->GetChunkId(), blockIndex);
        auto uncompressedBlock = BlockCache_->Find(blockId, EBlockType::UncompressedData);
        if (uncompressedBlock) {
            windowSlot.BlockPromise->Set(uncompressedBlock);
            windowSlot.Cached = true;
            TotalRemainingSize_ -= BlockInfos_[windowIndex].UncompressedDataSize;
        } else {
            TDispatcher::Get()->GetReaderInvoker()->Invoke(
                BIND(&TBlockFetcher::RequestBlocks,
                    MakeWeak(this),
                    std::vector<int> { windowIndex },
                    std::vector<int> { blockIndex },
                    static_cast<i64>(BlockInfos_[windowIndex].UncompressedDataSize)));
        }
    }
    auto returnValue = windowSlot.BlockPromise->ToFuture();

    auto hasBlock = windowSlot.BlockPromise->IsSet();
    if (--windowSlot.RemainingFetches == 0 && hasBlock) {
        TDispatcher::Get()->GetReaderInvoker()->Invoke(
            BIND(&TBlockFetcher::ReleaseBlock,
                MakeWeak(this),
                windowIndex));
    }

    --TotalRemainingFetches_;

    return returnValue;
}

void TBlockFetcher::DecompressBlocks(
    const std::vector<int>& windowIndexes,
    const std::vector<TBlock>& compressedBlocks)
{
    YCHECK(windowIndexes.size() == compressedBlocks.size());
    for (int i = 0; i < compressedBlocks.size(); ++i) {
        const auto& compressedBlock = compressedBlocks[i];
        int windowIndex = windowIndexes[i];
        const auto& blockInfo = BlockInfos_[windowIndex];
        int blockIndex = blockInfo.Index;
        TBlockId blockId(ChunkReader_->GetChunkId(), blockInfo.Index);

        LOG_DEBUG("Started decompressing block (BlockIndex: %v, WindowIndex: %v)",
            blockIndex,
            windowIndex);

        TSharedRef uncompressedBlock;
        {
            NProfiling::TCpuTimingGuard timer(&DecompressionTime);
            uncompressedBlock = Codec_->Decompress(compressedBlock.Data);
        }
        YCHECK(uncompressedBlock.Size() == blockInfo.UncompressedDataSize);

        auto& windowSlot = Window_[windowIndex];
        Window_[windowIndex].BlockPromise->Set(TBlock(uncompressedBlock));
        if (windowSlot.RemainingFetches == 0) {
            TDispatcher::Get()->GetReaderInvoker()->Invoke(
                BIND(&TBlockFetcher::ReleaseBlock,
                    MakeWeak(this),
                    windowIndex));
        }

        UncompressedDataSize_ += uncompressedBlock.Size();
        CompressedDataSize_ += compressedBlock.Size();

        LOG_DEBUG("Finished decompressing block (BlockIndex: %v, WindowIndex: %v, CompressedSize: %v, UncompressedSize: %v)",
            blockIndex,
            windowIndex,
            compressedBlock.Size(),
            uncompressedBlock.Size());

        if (Codec_->GetId() != NCompression::ECodec::None) {
            BlockCache_->Put(blockId, EBlockType::UncompressedData, TBlock(uncompressedBlock), Null);
        }
    }
}

void TBlockFetcher::FetchNextGroup(TAsyncSemaphoreGuard asyncSemaphoreGuard)
{
    std::vector<int> windowIndexes;
    std::vector<int> blockIndexes;
    i64 uncompressedSize = 0;
    i64 availableSlots = asyncSemaphoreGuard.GetSlots();
    while (FirstUnfetchedWindowIndex_ < BlockInfos_.size()) {
        const auto& blockInfo = BlockInfos_[FirstUnfetchedWindowIndex_];
        int blockIndex = blockInfo.Index;
        if (windowIndexes.empty() || uncompressedSize + blockInfo.UncompressedDataSize < availableSlots) {
            if (Window_[FirstUnfetchedWindowIndex_].FetchStarted.test_and_set()) {
                // This block has been already requested out of order.
                LOG_DEBUG("Skipping out of turn block (BlockIndex: %v, WindowIndex: %v)",
                    blockIndex,
                    FirstUnfetchedWindowIndex_);
                ++FirstUnfetchedWindowIndex_;
                continue;
            }
            Window_[FirstUnfetchedWindowIndex_].AsyncSemaphoreGuard =
                std::make_unique<TAsyncSemaphoreGuard>(asyncSemaphoreGuard.TransferSlots(
                    std::min(
                        static_cast<i64>(blockInfo.UncompressedDataSize),
                        asyncSemaphoreGuard.GetSlots())));

            TBlockId blockId(ChunkReader_->GetChunkId(), blockIndex);
            auto uncompressedBlock = BlockCache_->Find(blockId, EBlockType::UncompressedData);
            if (uncompressedBlock) {
                auto& slot = Window_[FirstUnfetchedWindowIndex_];
                slot.BlockPromise->Set(uncompressedBlock);
                slot.Cached = true;
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
        IsFetchingCompleted_ = true;
        return;
    }

    if (TotalRemainingSize_ > 0) {
        AsyncSemaphore_->AsyncAcquire(
            BIND(&TBlockFetcher::FetchNextGroup, MakeWeak(this)),
            TDispatcher::Get()->GetReaderInvoker(),
            std::min(static_cast<i64>(TotalRemainingSize_), Config_->GroupSize));
    }

    RequestBlocks(windowIndexes, blockIndexes, uncompressedSize);
}

void TBlockFetcher::MarkFailedBlocks(const std::vector<int>& windowIndexes, const TError& error)
{
    for (auto index : windowIndexes) {
        Window_[index].BlockPromise->Set(error);
    }
}

void TBlockFetcher::ReleaseBlock(int windowIndex)
{
    Window_[windowIndex].AsyncSemaphoreGuard.reset();
    Window_[windowIndex].BlockPromise->Reset();
    LOG_DEBUG("Releasing block (WindowIndex: %v, WindowSize: %v)",
        windowIndex,
        AsyncSemaphore_->GetFree());
}

void TBlockFetcher::RequestBlocks(
    const std::vector<int>& windowIndexes,
    const std::vector<int>& blockIndexes,
    i64 uncompressedSize)
{
    LOG_DEBUG("Requesting block group (Blocks: %v, UncompressedSize: %v)",
        blockIndexes,
        uncompressedSize);

    TotalRemainingSize_ -= uncompressedSize;

    auto blocksOrError = WaitFor(ChunkReader_->ReadBlocks(
        Config_->WorkloadDescriptor,
        ReadSessionId_,
        blockIndexes));

    if (!blocksOrError.IsOK()) {
        MarkFailedBlocks(windowIndexes, blocksOrError);
        return;
    }

    LOG_DEBUG("Got block group (Blocks: %v)",
        blockIndexes);

    CompressionInvoker_->Invoke(BIND(
        &TBlockFetcher::DecompressBlocks,
        MakeWeak(this),
        windowIndexes,
        blocksOrError.Value()));
}

bool TBlockFetcher::IsFetchingCompleted()
{
    return IsFetchingCompleted_;
}

i64 TBlockFetcher::GetUncompressedDataSize() const
{
    return UncompressedDataSize_;
}

i64 TBlockFetcher::GetCompressedDataSize() const
{
    return CompressedDataSize_;
}

TCodecTime TBlockFetcher::GetDecompressionTime() const
{
    return std::make_pair(Codec_->GetId(), DecompressionTime);
}

////////////////////////////////////////////////////////////////////////////////

TSequentialBlockFetcher::TSequentialBlockFetcher(
    TBlockFetcherConfigPtr config,
    std::vector<TBlockInfo> blockInfos,
    NConcurrency::TAsyncSemaphorePtr asyncSemaphore,
    IChunkReaderPtr chunkReader,
    IBlockCachePtr blockCache,
    NCompression::ECodec codecId,
    const TReadSessionId& sessionId)
    : TBlockFetcher(
        config,
        blockInfos,
        asyncSemaphore,
        chunkReader,
        blockCache,
        codecId,
        sessionId)
    , OriginalOrderBlockInfos_(blockInfos)
{ }

TFuture<TBlock> TSequentialBlockFetcher::FetchNextBlock()
{
    YCHECK(CurrentIndex_ < OriginalOrderBlockInfos_.size());
    return FetchBlock(OriginalOrderBlockInfos_[CurrentIndex_++].Index);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
