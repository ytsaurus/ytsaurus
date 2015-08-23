#include "stdafx.h"
#include "sequential_reader.h"
#include "config.h"
#include "private.h"
#include "dispatcher.h"
#include "block_cache.h"

#include <ytlib/node_tracker_client/node_directory.h>

#include <core/misc/string.h>

#include <core/compression/codec.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

TSequentialReader::TSequentialReader(
    TSequentialReaderConfigPtr config,
    std::vector<TBlockInfo> blockInfos,
    IChunkReaderPtr chunkReader,
    IBlockCachePtr blockCache,
    NCompression::ECodec codecId)
    : UncompressedDataSize_(0)
    , CompressedDataSize_(0)
    , Config_(std::move(config))
    , BlockInfos_(std::move(blockInfos))
    , ChunkReader_(std::move(chunkReader))
    , BlockCache_(std::move(blockCache))
    , AsyncSemaphore_(Config_->WindowSize)
    , Codec_(NCompression::GetCodec(codecId))
    , Logger(ChunkClientLogger)
{
    YCHECK(ChunkReader_);
    YCHECK(BlockCache_);
    YCHECK(!BlockInfos_.empty());

    Logger.AddTag("ChunkId: %v", ChunkReader_->GetChunkId());

    Window_.resize(BlockInfos_.size());

    std::vector<int> blockIndexes;
    for (const auto& info : BlockInfos_) {
        blockIndexes.push_back(info.Index);
    }

    LOG_TRACE("Creating sequential reader (Blocks: [%v])",
        JoinToString(blockIndexes));

    FetchNextGroup();
}

bool TSequentialReader::HasMoreBlocks() const
{
    return FirstReadyWindowIndex_ + 1 < Window_.size();
}

TSharedRef TSequentialReader::GetCurrentBlock()
{
    YCHECK(FirstReadyWindowIndex_ >= 0);

    const auto& slot = Window_[FirstReadyWindowIndex_];
    YCHECK(slot.Block.IsSet());
    return slot.Block.Get().Value();
}

TFuture<void> TSequentialReader::FetchNextBlock()
{
    YCHECK(HasMoreBlocks());

    if (FirstReadyWindowIndex_ >= 0) {
        auto& slot = Window_[FirstReadyWindowIndex_];
        if (!slot.Cached) {
            AsyncSemaphore_.Release(slot.Block.Get().Value().Size());
        }
        slot.Block.Reset();
    }

    ++FirstReadyWindowIndex_;
    return Window_[FirstReadyWindowIndex_].Block.ToFuture().As<void>();
}

void TSequentialReader::OnGotBlocks(
    const std::vector<int>& windowIndexes,
    const std::vector<int>& blockIndexes,
    const TErrorOr<std::vector<TSharedRef>>& blocksOrError)
{
    if (!blocksOrError.IsOK()) {
        MarkFailedBlocks(windowIndexes, blocksOrError);
        return;
    }

    LOG_DEBUG("Got block group (Blocks: [%v])",
        JoinToString(blockIndexes));

    TDispatcher::Get()->GetCompressionPoolInvoker()->Invoke(BIND(
        &TSequentialReader::DecompressBlocks,
        MakeWeak(this),
        windowIndexes,
        blocksOrError.Value()));
}

void TSequentialReader::DecompressBlocks(
    const std::vector<int>& windowIndexes,
    const std::vector<TSharedRef>& compressedBlocks)
{
    YCHECK(windowIndexes.size() == compressedBlocks.size());
    for (int i = 0; i < compressedBlocks.size(); ++i) {
        const auto& compressedBlock = compressedBlocks[i];
        int windowIndex = windowIndexes[i];
        const auto& blockInfo = BlockInfos_[windowIndex];
        TBlockId blockId(ChunkReader_->GetChunkId(), blockInfo.Index);

        LOG_DEBUG("Started decompressing block (Block: %v)",
            blockInfo.Index);

        auto uncompressedBlock = Codec_->Decompress(compressedBlock);
        YCHECK(uncompressedBlock.Size() == blockInfo.UncompressedDataSize);

        Window_[windowIndex].Block.Set(uncompressedBlock);

        UncompressedDataSize_ += uncompressedBlock.Size();
        CompressedDataSize_ += compressedBlock.Size();

        LOG_DEBUG("Finished decompressing block (Block: %v, CompressedSize: %v, UncompressedSize: %v)",
            blockInfo.Index,
            compressedBlock.Size(),
            uncompressedBlock.Size());

        if (Codec_->GetId() != NCompression::ECodec::None) {
            BlockCache_->Put(blockId, EBlockType::UncompressedData, uncompressedBlock, Null);
        }
    }
}

void TSequentialReader::FetchNextGroup()
{
    std::vector<int> windowIndexes;
    std::vector<int> blockIndexes;
    i64 uncompressedSize = 0;
    while (FirstUnfetchedWindowIndex_ < BlockInfos_.size()) {
        const auto& blockInfo = BlockInfos_[FirstUnfetchedWindowIndex_];
        TBlockId blockId(ChunkReader_->GetChunkId(), blockInfo.Index);
        auto uncompressedBlock = BlockCache_->Find(blockId, EBlockType::UncompressedData);
        if (uncompressedBlock) {
            auto& slot = Window_[FirstUnfetchedWindowIndex_];
            slot.Block.Set(uncompressedBlock);
            slot.Cached = true;
        } else {
            // Do not exceed group size if possible.
            if (!windowIndexes.empty() && uncompressedSize + blockInfo.UncompressedDataSize > Config_->GroupSize) {
                break;
            }
            windowIndexes.push_back(FirstUnfetchedWindowIndex_);
            blockIndexes.push_back(blockInfo.Index);
            uncompressedSize += blockInfo.UncompressedDataSize;
        }

        ++FirstUnfetchedWindowIndex_;
    }

    if (windowIndexes.empty()) {
        FetchingComplete_.Set();
        return;
    }

    AsyncSemaphore_.GetReadyEvent().Subscribe(
        BIND(&TSequentialReader::RequestBlocks,
            MakeWeak(this),
            windowIndexes,
            blockIndexes,
            uncompressedSize));
}

void TSequentialReader::MarkFailedBlocks(const std::vector<int>& windowIndexes, const TError& error)
{
    for (auto index : windowIndexes) {
        Window_[index].Block.Set(error);
    }
}

void TSequentialReader::RequestBlocks(
    const std::vector<int>& windowIndexes,
    const std::vector<int>& blockIndexes,
    i64 uncompressedSize,
    const TError& error)
{
    if (!error.IsOK()) {
        MarkFailedBlocks(windowIndexes, error);
        return;
    }

    LOG_DEBUG("Requesting block group (Blocks: [%v], UncompressedSize: %v)",
        JoinToString(blockIndexes),
        uncompressedSize);

    AsyncSemaphore_.Acquire(uncompressedSize);
    ChunkReader_->ReadBlocks(blockIndexes).Subscribe(
        BIND(&TSequentialReader::OnGotBlocks,
            MakeWeak(this),
            windowIndexes,
            blockIndexes));

    FetchNextGroup();
}

TFuture<void> TSequentialReader::GetFetchingCompletedEvent()
{
    return FetchingComplete_;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
