#include "stdafx.h"
#include "sequential_reader.h"
#include "config.h"
#include "private.h"
#include "dispatcher.h"
#include "block_cache.h"

#include <core/misc/string.h>

#include <core/compression/codec.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;

///////////////////////////////////////////////////////////////////////////////

TSequentialReader::TSequentialReader(
    TSequentialReaderConfigPtr config,
    std::vector<TBlockInfo> blockInfos,
    IReaderPtr chunkReader,
    IBlockCachePtr uncompressedBlockCache,
    NCompression::ECodec codecId)
    : UncompressedDataSize_(0)
    , CompressedDataSize_(0)
    , Config_(config)
    , BlockInfos_(std::move(blockInfos))
    , ChunkReader_(std::move(chunkReader))
    , UncompressedBlockCache_(std::move(uncompressedBlockCache))
    , AsyncSemaphore_(config->WindowSize)
    , Codec_(NCompression::GetCodec(codecId))
    , Logger(ChunkClientLogger)
{
    VERIFY_INVOKER_AFFINITY(TDispatcher::Get()->GetReaderInvoker(), ReaderThread);
    YCHECK(ChunkReader_);

    Logger.AddTag("ChunkId: %v", ChunkReader_->GetChunkId());

    std::vector<int> blockIndexes;
    for (const auto& info : BlockInfos_) {
        BlockWindow_.push_back(NewPromise<TSharedRef>());
        blockIndexes.push_back(info.Index);
    }

    LOG_DEBUG("Creating sequential reader (Blocks: [%v])",
        JoinToString(blockIndexes));

    TDispatcher::Get()->GetReaderInvoker()->Invoke(BIND(
        &TSequentialReader::FetchNextGroup,
        MakeWeak(this)));
}

bool TSequentialReader::HasNext() const
{
    // No thread affinity - can be called from
    // ContinueNextRow of NTableClient::TChunkReader.
    return FirstReadyWindowIndex_ + 1 < BlockWindow_.size();
}

TSharedRef TSequentialReader::GetBlock()
{
    // No thread affinity - can be called from
    // ContinueNextRow of NTableClient::TChunkReader.
    YCHECK(!State_.HasRunningOperation());
    YCHECK(FirstReadyWindowIndex_ >= 0);
    YCHECK(BlockWindow_[FirstReadyWindowIndex_].IsSet());

    return BlockWindow_[FirstReadyWindowIndex_].Get();
}

TAsyncError TSequentialReader::AsyncNextBlock()
{
    // No thread affinity - can be called from
    // ContinueNextRow of NTableClient::TChunkReader.

    YCHECK(HasNext());
    YCHECK(!State_.HasRunningOperation());

    if (FirstReadyWindowIndex_ >= 0) {
        AsyncSemaphore_.Release(BlockWindow_[FirstReadyWindowIndex_].Get().Size());
        BlockWindow_[FirstReadyWindowIndex_].Reset();
    }

    State_.StartOperation();

    ++FirstReadyWindowIndex_;

    auto this_ = MakeStrong(this);
    BlockWindow_[FirstReadyWindowIndex_].Subscribe(
        BIND([=] (TSharedRef) {
            this_->State_.FinishOperation();
        }));

    return State_.GetOperationError();
}

void TSequentialReader::OnGotBlocks(
    const std::vector<int>& windowIndexes,
    const std::vector<int>& blockIndexes,
    IReader::TReadBlocksResult readResult)
{
    VERIFY_THREAD_AFFINITY(ReaderThread);

    if (!State_.IsActive())
        return;

    if (!readResult.IsOK()) {
        State_.Fail(readResult);
        return;
    }

    LOG_DEBUG("Got block group (Blocks: %v)",
        JoinToString(blockIndexes));

    TDispatcher::Get()->GetCompressionPoolInvoker()->Invoke(BIND(
        &TSequentialReader::DecompressBlocks,
        MakeWeak(this),
        windowIndexes,
        readResult.Value()));
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
        BlockWindow_[windowIndex].Set(uncompressedBlock);

        UncompressedDataSize_ += uncompressedBlock.Size();
        CompressedDataSize_ += compressedBlock.Size();

        i64 delta = uncompressedBlock.Size() - BlockInfos_[windowIndex].Size;

        if (delta > 0) {
            AsyncSemaphore_.Acquire(delta);
        } else {
            AsyncSemaphore_.Release(-delta);
        }

        LOG_DEBUG("Finished decompressing block (Block: %v, CompressedSize: %v, UncompressedSize: %v)",
            blockInfo.Index,
            compressedBlock.Size(),
            uncompressedBlock.Size());

        if (Codec_->GetId() != NCompression::ECodec::None) {
            UncompressedBlockCache_->Put(blockId, uncompressedBlock, Null);
        }
    }
}

void TSequentialReader::FetchNextGroup()
{
    VERIFY_THREAD_AFFINITY(ReaderThread);

    std::vector<int> windowIndexes;
    std::vector<int> blockIndexes;
    i64 groupSize = 0;
    while (FirstUnfetchedWindowIndex_ < BlockInfos_.size()) {
        const auto& blockInfo = BlockInfos_[FirstUnfetchedWindowIndex_];
        TBlockId blockId(ChunkReader_->GetChunkId(), blockInfo.Index);

        auto uncompressedBlock = UncompressedBlockCache_->Find(blockId);
        if (uncompressedBlock) {
            BlockWindow_[FirstUnfetchedWindowIndex_].Set(uncompressedBlock);
        } else {
            // Do not exceed group size if possible.
            if (!windowIndexes.empty() && groupSize + blockInfo.Size > Config_->GroupSize) {
                break;
            }
            windowIndexes.push_back(FirstUnfetchedWindowIndex_);
            blockIndexes.push_back(blockInfo.Index);
            groupSize += blockInfo.Size;
        }

        ++FirstUnfetchedWindowIndex_;
    }

    if (groupSize == 0) {
        FetchingComplete_.Set();
        return;
    }

    LOG_DEBUG("Requesting block group (Blocks: [%v], GroupSize: %v)",
        JoinToString(blockIndexes),
        groupSize);

    AsyncSemaphore_.GetReadyEvent().Subscribe(
        BIND(&TSequentialReader::RequestBlocks,
            MakeWeak(this),
            windowIndexes,
            blockIndexes,
            groupSize)
        .Via(TDispatcher::Get()->GetReaderInvoker()));
}

void TSequentialReader::RequestBlocks(
    const std::vector<int>& windowIndexes,
    const std::vector<int>& blockIndexes,
    i64 groupSize)
{
    AsyncSemaphore_.Acquire(groupSize);
    ChunkReader_->ReadBlocks(blockIndexes).Subscribe(
        BIND(&TSequentialReader::OnGotBlocks,
            MakeWeak(this),
            windowIndexes,
            blockIndexes)
            .Via(TDispatcher::Get()->GetReaderInvoker()));

    TDispatcher::Get()->GetReaderInvoker()->Invoke(BIND(
        &TSequentialReader::FetchNextGroup,
        MakeWeak(this)));
}

TFuture<void> TSequentialReader::GetFetchingCompleteEvent()
{
    return FetchingComplete_;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
