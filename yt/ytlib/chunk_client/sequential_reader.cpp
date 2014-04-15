#include "stdafx.h"
#include "sequential_reader.h"
#include "config.h"
#include "private.h"
#include "dispatcher.h"

#include <core/compression/codec.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;

///////////////////////////////////////////////////////////////////////////////

TSequentialReader::TSequentialReader(
    TSequentialReaderConfigPtr config,
    std::vector<TBlockInfo>&& blocks,
    IAsyncReaderPtr chunkReader,
    NCompression::ECodec codecId)
    : UncompressedDataSize_(0)
    , CompressedDataSize_(0)
    , BlockSequence(blocks)
    , Config(config)
    , ChunkReader(chunkReader)
    , AsyncSemaphore(config->WindowSize)
    , NextSequenceIndex(0)
    , NextUnfetchedIndex(0)
    , FetchingCompleteEvent(NewPromise())
    , Codec(NCompression::GetCodec(codecId))
    , Logger(ChunkReaderLogger)
{
    VERIFY_INVOKER_AFFINITY(TDispatcher::Get()->GetReaderInvoker(), ReaderThread);

    Logger.AddTag(Sprintf("ChunkId: %s", ~ToString(ChunkReader->GetChunkId())));

    YCHECK(ChunkReader);

    LOG_DEBUG("Creating sequential reader (BlockCount: %d)",
        static_cast<int>(blocks.size()));

    BlockWindow.reserve(BlockSequence.size());
    for (int i = 0; i < BlockSequence.size(); ++i) {
        BlockWindow.push_back(NewPromise<TSharedRef>());
    }

    TDispatcher::Get()->GetReaderInvoker()->Invoke(BIND(
        &TSequentialReader::FetchNextGroup,
        MakeWeak(this)));
}

bool TSequentialReader::HasNext() const
{
    // No thread affinity - can be called from
    // ContinueNextRow of NTableClient::TChunkReader.
    return NextSequenceIndex < BlockWindow.size();
}

TSharedRef TSequentialReader::GetBlock()
{
    // No thread affinity - can be called from
    // ContinueNextRow of NTableClient::TChunkReader.
    YCHECK(!State.HasRunningOperation());
    YCHECK(NextSequenceIndex > 0);
    YCHECK(BlockWindow[NextSequenceIndex - 1].IsSet());

    return BlockWindow[NextSequenceIndex - 1].Get();
}

TAsyncError TSequentialReader::AsyncNextBlock()
{
    // No thread affinity - can be called from
    // ContinueNextRow of NTableClient::TChunkReader.

    YCHECK(HasNext());
    YCHECK(!State.HasRunningOperation());

    if (NextSequenceIndex > 0) {
        AsyncSemaphore.Release(BlockWindow[NextSequenceIndex - 1].Get().Size());
        BlockWindow[NextSequenceIndex - 1].Reset();
    }

    State.StartOperation();

    auto this_ = MakeStrong(this);
    BlockWindow[NextSequenceIndex].Subscribe(
        BIND([=] (TSharedRef) {
            this_->State.FinishOperation();
        }));

    ++NextSequenceIndex;

    return State.GetOperationError();
}

void TSequentialReader::OnGotBlocks(
    int firstSequenceIndex,
    IAsyncReader::TReadResult readResult)
{
    VERIFY_THREAD_AFFINITY(ReaderThread);

    if (!State.IsActive())
        return;

    if (!readResult.IsOK()) {
        State.Fail(readResult);
        LOG_WARNING("Failed to read block group starting from %d", firstSequenceIndex);
        return;
    }

    const auto& blocks = readResult.Value();

    LOG_DEBUG(
        "Got block group (FirstIndex: %d, BlockCount: %d)",
        firstSequenceIndex,
        static_cast<int>(blocks.size()));

    TDispatcher::Get()->GetCompressionInvoker()->Invoke(BIND(
        &TSequentialReader::DecompressBlocks,
        MakeWeak(this),
        firstSequenceIndex,
        readResult));
}

void TSequentialReader::DecompressBlocks(
    int blockIndex,
    const IAsyncReader::TReadResult& readResult)
{
    const auto& blocks = readResult.Value();
    for (int i = 0; i < blocks.size(); ++i, ++blockIndex) {
        const auto& block = blocks[i];
        auto data = Codec->Decompress(block);
        BlockWindow[blockIndex].Set(data);

        UncompressedDataSize_ += data.Size();
        CompressedDataSize_ += block.Size();

        i64 delta = data.Size() - BlockSequence[blockIndex].Size;

        if (delta > 0)
            AsyncSemaphore.Acquire(delta);
        else
            AsyncSemaphore.Release(-delta);

        LOG_DEBUG("Decompressed block %d (CompressedSize: %" PRId64 ", UncompressedSize: %" PRId64 ")", 
            blockIndex,
            static_cast<i64>(block.Size()),
            static_cast<i64>(data.Size()));
    }
}

void TSequentialReader::FetchNextGroup()
{
    VERIFY_THREAD_AFFINITY(ReaderThread);

    // ToDo(psushin): maybe use TSmallVector here?
    auto firstUnfetched = NextUnfetchedIndex;
    std::vector<int> blockIndexes;
    int groupSize = 0;
    while (NextUnfetchedIndex < BlockSequence.size()) {
        auto& blockInfo = BlockSequence[NextUnfetchedIndex];

        if (!blockIndexes.empty() && groupSize + blockInfo.Size > Config->GroupSize) {
            // Do not exceed group size if possible.
            break;
        }

        blockIndexes.push_back(blockInfo.Index);
        groupSize += blockInfo.Size;
        ++NextUnfetchedIndex;
    }

    if (!groupSize) {
        FetchingCompleteEvent.Set();
        return;
    }

    LOG_DEBUG(
        "Requesting block group (FirstIndex: %d, BlockCount: %d, GroupSize: %d)",
        firstUnfetched,
        static_cast<int>(blockIndexes.size()),
        groupSize);

    AsyncSemaphore.GetReadyEvent().Subscribe(
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
    int groupSize)
{
    AsyncSemaphore.Acquire(groupSize);
    ChunkReader->AsyncReadBlocks(blockIndexes).Subscribe(
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
    return FetchingCompleteEvent;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
