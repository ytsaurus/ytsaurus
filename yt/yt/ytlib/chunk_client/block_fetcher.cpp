#include "block_fetcher.h"
#include "private.h"
#include "block_cache.h"
#include "config.h"
#include "dispatcher.h"
#include "chunk_reader_memory_manager.h"
#include "chunk_reader_statistics.h"

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/core/compression/codec.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/rpc/dispatcher.h>

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
    const TClientBlockReadOptions& blockReadOptions)
    : Config_(std::move(config))
    , BlockInfos_(std::move(blockInfos))
    , ChunkReader_(std::move(chunkReader))
    , BlockCache_(std::move(blockCache))
    , CompressionInvoker_(
        codecId == NCompression::ECodec::None
        ? nullptr
        : GetCompressionInvoker(blockReadOptions.WorkloadDescriptor))
    , ReaderInvoker_(CreateSerializedInvoker(TDispatcher::Get()->GetReaderInvoker()))
    , CompressionRatio_(compressionRatio)
    , MemoryManager_(std::move(memoryManager))
    , Codec_(NCompression::GetCodec(codecId))
    , BlockReadOptions_(blockReadOptions)
    , Logger(ChunkClientLogger)
{
    YT_VERIFY(ChunkReader_);
    YT_VERIFY(BlockCache_);
    YT_VERIFY(!BlockInfos_.empty());

    Logger.AddTag("ChunkId: %v", ChunkReader_->GetChunkId());
    if (BlockReadOptions_.ReadSessionId) {
        Logger.AddTag("ReadSessionId: %v", BlockReadOptions_.ReadSessionId);
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
    std::vector<int> blockIndexes;
    for (int leftIndex = 0, rightIndex = 0; leftIndex != BlockInfos_.size(); leftIndex = rightIndex) {
        const auto& currentBlock = BlockInfos_[leftIndex];
        while (rightIndex != BlockInfos_.size() && BlockInfos_[rightIndex].Index == currentBlock.Index) {
            ++rightIndex;
        }

        // Contrary would mean that the same block was requested twice with different priorities.
        YT_VERIFY(BlockIndexToWindowIndex_.find(currentBlock.Index) == BlockIndexToWindowIndex_.end());

        int windowIndex = BlockIndexToWindowIndex_.size();
        BlockIndexToWindowIndex_[currentBlock.Index] = windowIndex;
        Window_[windowIndex].RemainingFetches = rightIndex - leftIndex;
        BlockInfos_[windowIndex] = BlockInfos_[leftIndex]; // Similar to std::unique
        blockIndexes.push_back(currentBlock.Index);
    }

    // Now Window_ and BlockInfos_ correspond to each other.
    BlockInfos_.resize(WindowSize_);

    size_t totalBlockUncompressedSize = 0;
    for (const auto& blockInfo : BlockInfos_) {
        totalBlockUncompressedSize += blockInfo.UncompressedDataSize;
    }

    MemoryManager_->SetTotalSize(totalBlockUncompressedSize + Config_->WindowSize);
    MemoryManager_->SetPrefetchMemorySize(std::min<i64>(Config_->WindowSize, TotalRemainingSize_));

    YT_LOG_DEBUG("Creating block fetcher (Blocks: %v)",
        blockIndexes);

    YT_VERIFY(TotalRemainingSize_ > 0);

    FetchNextGroupMemoryFuture_ =
        MemoryManager_->AsyncAquire(
            std::min(TotalRemainingSize_.load(), Config_->GroupSize));
    FetchNextGroupMemoryFuture_.Subscribe(BIND(&TBlockFetcher::FetchNextGroup, MakeWeak(this))
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

    YT_VERIFY(windowSlot.RemainingFetches > 0);
    if (!windowSlot.FetchStarted.test_and_set()) {
        YT_LOG_DEBUG("Fetching block out of turn (BlockIndex: %v, WindowIndex: %v)",
            blockIndex,
            windowIndex);

        windowSlot.MemoryUsageGuard = MemoryManager_->Acquire(BlockInfos_[windowIndex].UncompressedDataSize);

        TBlockId blockId(ChunkReader_->GetChunkId(), blockIndex);
        auto uncompressedBlock = BlockCache_->Find(blockId, EBlockType::UncompressedData);
        if (uncompressedBlock) {
            auto managedBlock = New<TMemoryManagedData>(
                uncompressedBlock.Data, std::move(windowSlot.MemoryUsageGuard));
            GetBlockPromise(windowSlot).Set(
                TBlock(TSharedRef(managedBlock->Data, managedBlock)));
            TotalRemainingSize_ -= BlockInfos_[windowIndex].UncompressedDataSize;
        } else {
            ReaderInvoker_->Invoke(BIND(
                &TBlockFetcher::RequestBlocks,
                MakeWeak(this),
                std::vector{windowIndex},
                std::vector{blockIndex},
                static_cast<i64>(BlockInfos_[windowIndex].UncompressedDataSize)));
        }
    }

    auto blockPromise = GetBlockPromise(windowSlot);
    auto returnValue = blockPromise.ToFuture();
    auto hasBlock = blockPromise.IsSet();
    if (--windowSlot.RemainingFetches == 0 && hasBlock) {
        ReaderInvoker_->Invoke(
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
    YT_VERIFY(windowIndexes.size() == compressedBlocks.size());
    for (int i = 0; i < static_cast<int>(compressedBlocks.size()); ++i) {
        const auto& compressedBlock = compressedBlocks[i];
        int windowIndex = windowIndexes[i];
        const auto& blockInfo = BlockInfos_[windowIndex];
        int blockIndex = blockInfo.Index;
        TBlockId blockId(ChunkReader_->GetChunkId(), blockInfo.Index);

        TSharedRef uncompressedBlock;
        if (Codec_->GetId() == NCompression::ECodec::None) {
            uncompressedBlock = compressedBlock.Data;
        } else {
            YT_LOG_DEBUG("Started decompressing block (BlockIndex: %v, WindowIndex: %v, Codec: %v)",
                blockIndex,
                windowIndex,
                Codec_->GetId());

            {
                TWallTimer timer;
                uncompressedBlock = Codec_->Decompress(compressedBlock.Data);
                DecompressionTime_ += timer.GetElapsedValue();
                YT_VERIFY(uncompressedBlock.Size() == blockInfo.UncompressedDataSize);
            }

            YT_LOG_DEBUG("Finished decompressing block (BlockIndex: %v, WindowIndex: %v, CompressedSize: %v, UncompressedSize: %v, Codec: %v)",
                blockIndex,
                windowIndex,
                compressedBlock.Size(),
                uncompressedBlock.Size(),
                Codec_->GetId());
        }

        auto& windowSlot = Window_[windowIndex];
        auto managedBlock = New<TMemoryManagedData>(uncompressedBlock, std::move(windowSlot.MemoryUsageGuard));
        GetBlockPromise(windowSlot).Set(
            TBlock(TSharedRef(managedBlock->Data, managedBlock)));
        if (windowSlot.RemainingFetches == 0) {
            ReaderInvoker_->Invoke(
                BIND(&TBlockFetcher::ReleaseBlock, MakeWeak(this), windowIndex));
        }

        UncompressedDataSize_ += uncompressedBlock.Size();
        CompressedDataSize_ += compressedBlock.Size();

        BlockCache_->Put(blockId, EBlockType::UncompressedData, TBlock(uncompressedBlock), std::nullopt);
    }
}

void TBlockFetcher::FetchNextGroup(TErrorOr<TMemoryUsageGuardPtr> memoryUsageGuardOrError)
{
    if (!memoryUsageGuardOrError.IsOK()) {
        YT_LOG_INFO(memoryUsageGuardOrError, "Failed to acquire memory in chunk reader memory manager");
        return;
    }

    const auto& memoryUsageGuard = memoryUsageGuardOrError.Value();

    std::vector<int> windowIndexes;
    std::vector<int> blockIndexes;
    i64 uncompressedSize = 0;
    i64 availableSlots = memoryUsageGuard->Guard.GetSlots();
    while (FirstUnfetchedWindowIndex_ < BlockInfos_.size()) {
        const auto& blockInfo = BlockInfos_[FirstUnfetchedWindowIndex_];
        int blockIndex = blockInfo.Index;
        if (windowIndexes.empty() || uncompressedSize + blockInfo.UncompressedDataSize < availableSlots) {
            if (Window_[FirstUnfetchedWindowIndex_].FetchStarted.test_and_set()) {
                // This block has been already requested out of order.
                YT_LOG_DEBUG("Skipping out of turn block (BlockIndex: %v, WindowIndex: %v)",
                    blockIndex,
                    FirstUnfetchedWindowIndex_);
                ++FirstUnfetchedWindowIndex_;
                continue;
            }

            auto transferred = memoryUsageGuard->Guard.TransferSlots(
                std::min(
                    static_cast<i64>(blockInfo.UncompressedDataSize),
                    memoryUsageGuard->Guard.GetSlots()));
            Window_[FirstUnfetchedWindowIndex_].MemoryUsageGuard = New<TMemoryUsageGuard>(
                std::move(transferred),
                memoryUsageGuard->MemoryManager);

            TBlockId blockId(ChunkReader_->GetChunkId(), blockIndex);
            auto uncompressedBlock = BlockCache_->Find(blockId, EBlockType::UncompressedData);
            if (uncompressedBlock) {
                auto& windowSlot = Window_[FirstUnfetchedWindowIndex_];
                auto managedBlock = New<TMemoryManagedData>(
                    uncompressedBlock.Data, std::move(windowSlot.MemoryUsageGuard));
                GetBlockPromise(windowSlot).Set(
                    TBlock(TSharedRef(managedBlock->Data, managedBlock)));
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
        MemoryManager_->SetPrefetchMemorySize(std::min<i64>(TotalRemainingSize_, Config_->GroupSize));
        FetchNextGroupMemoryFuture_ =
            MemoryManager_->AsyncAquire(std::min<i64>(TotalRemainingSize_, Config_->GroupSize));
        FetchNextGroupMemoryFuture_.Subscribe(BIND(&TBlockFetcher::FetchNextGroup, MakeWeak(this))
            .Via(ReaderInvoker_));
    }

    RequestBlocks(windowIndexes, blockIndexes, uncompressedSize);
}

void TBlockFetcher::MarkFailedBlocks(const std::vector<int>& windowIndexes, const TError& error)
{
    for (auto index : windowIndexes) {
        GetBlockPromise(Window_[index]).Set(error);
    }
}

void TBlockFetcher::ReleaseBlock(int windowIndex)
{
    auto& windowSlot = Window_[windowIndex];
    ResetBlockPromise(windowSlot);
    YT_LOG_DEBUG("Releasing block (WindowIndex: %v)",
        windowIndex);
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
    const std::vector<int>& windowIndexes,
    const std::vector<int>& blockIndexes,
    i64 uncompressedSize)
{
    YT_LOG_DEBUG("Requesting block group (Blocks: %v, UncompressedSize: %v)",
        MakeShrunkFormattableView(blockIndexes, TDefaultFormatter(), 3),
        uncompressedSize);

    TotalRemainingSize_ -= uncompressedSize;

    auto future = ChunkReader_->ReadBlocks(
        BlockReadOptions_,
        blockIndexes,
        static_cast<i64>(uncompressedSize * CompressionRatio_));

    // NB: Handling OnGotBlocks in an arbitrary thread seems OK.
    future.Subscribe(
        BIND(
            &TBlockFetcher::OnGotBlocks,
            MakeWeak(this),
            windowIndexes,
            blockIndexes));
}

void TBlockFetcher::OnGotBlocks(
    const std::vector<int>& windowIndexes,
    const std::vector<int>& blockIndexes,
    const TErrorOr<std::vector<TBlock>>& blocksOrError)
{
    if (!blocksOrError.IsOK()) {
        MarkFailedBlocks(windowIndexes, blocksOrError);
        return;
    }

    YT_LOG_DEBUG("Got block group (Blocks: %v)",
        MakeShrunkFormattableView(blockIndexes, TDefaultFormatter(), 3));

    const auto& blocks = blocksOrError.Value();
    if (Codec_->GetId() == NCompression::ECodec::None) {
        DecompressBlocks(windowIndexes, blocks);
    } else {
        CompressionInvoker_->Invoke(BIND(
            &TBlockFetcher::DecompressBlocks,
            MakeWeak(this),
            windowIndexes,
            blocks));
    }
}

bool TBlockFetcher::IsFetchingCompleted()
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
    const TClientBlockReadOptions& blockReadOptions)
    : TBlockFetcher(
        config,
        blockInfos,
        memoryManager,
        chunkReader,
        blockCache,
        codecId,
        compressionRatio,
        blockReadOptions)
    , OriginalOrderBlockInfos_(blockInfos)
{ }

TFuture<TBlock> TSequentialBlockFetcher::FetchNextBlock()
{
    YT_VERIFY(CurrentIndex_ < OriginalOrderBlockInfos_.size());
    return FetchBlock(OriginalOrderBlockInfos_[CurrentIndex_++].Index);
}

i64 TSequentialBlockFetcher::GetNextBlockSize() const
{
    YT_VERIFY(CurrentIndex_ < OriginalOrderBlockInfos_.size());
    return OriginalOrderBlockInfos_[CurrentIndex_].UncompressedDataSize;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
