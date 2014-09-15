#include "stdafx.h"
#include "sequential_reader.h"
#include "config.h"
#include "private.h"
#include "dispatcher.h"

#include <core/misc/string.h>

#include <core/compression/codec.h>

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
        blockIndexes.push_back(info.Index);
    }

    LOG_DEBUG("Creating sequential reader (Blocks: [%v])",
        JoinToString(blockIndexes));

    BlockWindow_.reserve(BlockInfos_.size());
    for (int i = 0; i < BlockInfos_.size(); ++i) {
        BlockWindow_.push_back(NewPromise<TSharedRef>());
    }

    TDispatcher::Get()->GetReaderInvoker()->Invoke(BIND(
        &TSequentialReader::FetchNextGroup,
        MakeWeak(this)));
}

bool TSequentialReader::HasNext() const
{
    // No thread affinity - can be called from
    // ContinueNextRow of NTableClient::TChunkReader.
    return NextSequenceIndex_ < BlockWindow_.size();
}

TSharedRef TSequentialReader::GetBlock()
{
    // No thread affinity - can be called from
    // ContinueNextRow of NTableClient::TChunkReader.
    YCHECK(!State_.HasRunningOperation());
    YCHECK(NextSequenceIndex_ > 0);
    YCHECK(BlockWindow_[NextSequenceIndex_ - 1].IsSet());

    return BlockWindow_[NextSequenceIndex_ - 1].Get();
}

TAsyncError TSequentialReader::AsyncNextBlock()
{
    // No thread affinity - can be called from
    // ContinueNextRow of NTableClient::TChunkReader.

    YCHECK(HasNext());
    YCHECK(!State_.HasRunningOperation());

    if (NextSequenceIndex_ > 0) {
        AsyncSemaphore_.Release(BlockWindow_[NextSequenceIndex_ - 1].Get().Size());
        BlockWindow_[NextSequenceIndex_ - 1].Reset();
    }

    State_.StartOperation();

    auto this_ = MakeStrong(this);
    BlockWindow_[NextSequenceIndex_].Subscribe(
        BIND([=] (TSharedRef) {
            this_->State_.FinishOperation();
        }));

    ++NextSequenceIndex_;

    return State_.GetOperationError();
}

void TSequentialReader::OnGotBlocks(
    int firstSequenceIndex,
    IReader::TReadBlocksResult readResult)
{
    VERIFY_THREAD_AFFINITY(ReaderThread);

    if (!State_.IsActive())
        return;

    if (!readResult.IsOK()) {
        State_.Fail(readResult);
        return;
    }

    const auto& blocks = readResult.Value();

    LOG_DEBUG("Got block group (FirstIndex: %v, BlockCount: %v)",
        firstSequenceIndex,
        blocks.size());

    TDispatcher::Get()->GetCompressionPoolInvoker()->Invoke(BIND(
        &TSequentialReader::DecompressBlocks,
        MakeWeak(this),
        firstSequenceIndex,
        readResult));
}

void TSequentialReader::DecompressBlocks(
    int blockIndex,
    const IReader::TReadBlocksResult& readResult)
{
    const auto& readBlocks = readResult.Value();
    for (int i = 0; i < readBlocks.size(); ++i, ++blockIndex) {
        const auto& block = readBlocks[i];
        const auto& blockInfo = BlockInfos_[blockIndex];

        LOG_DEBUG("Started decompressing block (Block: %v)",
            blockInfo.Index);

        auto data = Codec_->Decompress(block);
        BlockWindow_[blockIndex].Set(data);

        UncompressedDataSize_ += data.Size();
        CompressedDataSize_ += block.Size();

        i64 delta = data.Size() - BlockInfos_[blockIndex].Size;

        if (delta > 0) {
            AsyncSemaphore_.Acquire(delta);
        } else {
            AsyncSemaphore_.Release(-delta);
        }

        LOG_DEBUG("Finished decompressing block (BlockIndex: %v, CompressedSize: %v, UncompressedSize: %v)", 
            blockInfo.Index,
            block.Size(),
            data.Size());
    }
}

void TSequentialReader::FetchNextGroup()
{
    VERIFY_THREAD_AFFINITY(ReaderThread);

    // ToDo(psushin): maybe use SmallVector here?
    auto firstUnfetched = NextUnfetchedIndex_;
    std::vector<int> blockIndexes;
    i64 groupSize = 0;
    while (NextUnfetchedIndex_ < BlockInfos_.size()) {
        auto& blockInfo = BlockInfos_[NextUnfetchedIndex_];

        if (!blockIndexes.empty() && groupSize + blockInfo.Size > Config_->GroupSize) {
            // Do not exceed group size if possible.
            break;
        }

        blockIndexes.push_back(blockInfo.Index);
        groupSize += blockInfo.Size;
        ++NextUnfetchedIndex_;
    }

    if (!groupSize) {
        FetchingCompleteEvent_.Set();
        return;
    }

    LOG_DEBUG("Requesting block group (FirstIndex: %v, BlockCount: %v, GroupSize: %v)",
        firstUnfetched,
        blockIndexes.size(),
        groupSize);

    AsyncSemaphore_.GetReadyEvent().Subscribe(
        BIND(&TSequentialReader::RequestBlocks,
            MakeWeak(this),
            firstUnfetched,
            blockIndexes,
            groupSize)
        .Via(TDispatcher::Get()->GetReaderInvoker()));
}

void TSequentialReader::RequestBlocks(
    int firstIndex,
    const std::vector<int>& blockIndexes,
    i64 groupSize)
{
    AsyncSemaphore_.Acquire(groupSize);
    ChunkReader_->ReadBlocks(blockIndexes).Subscribe(
        BIND(&TSequentialReader::OnGotBlocks,
            MakeWeak(this),
            firstIndex)
            .Via(TDispatcher::Get()->GetReaderInvoker()));

    TDispatcher::Get()->GetReaderInvoker()->Invoke(BIND(
        &TSequentialReader::FetchNextGroup,
        MakeWeak(this)));
}

TFuture<void> TSequentialReader::GetFetchingCompleteEvent()
{
    return FetchingCompleteEvent_;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
