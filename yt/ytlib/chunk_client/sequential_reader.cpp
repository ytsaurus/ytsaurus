#include "stdafx.h"
#include "sequential_reader.h"
#include "config.h"
#include "private.h"

namespace NYT {
namespace NChunkClient {

using namespace NChunkHolder::NProto;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkReaderLogger;

///////////////////////////////////////////////////////////////////////////////

TSequentialReader::TSequentialReader(
    TSequentialReaderConfigPtr config,
    const std::vector<int>& blockIndexes,
    IAsyncReaderPtr chunkReader,
    const TBlocksExt& blocksExt)
    : BlockIndexSequence(blockIndexes)
    , BlocksExt(blocksExt)
    , Config(config)
    , ChunkReader(chunkReader)
    , AsyncSemaphore(config->WindowSize)
    , NextSequenceIndex(0)
    , NextUnfetchedIndex(0)
{
    VERIFY_INVOKER_AFFINITY(ReaderThread->GetInvoker(), ReaderThread);

    YASSERT(ChunkReader);
    YASSERT(blockIndexes.size() > 0);
    YASSERT(blockIndexes.size() <= BlocksExt.blocks_size());

    LOG_DEBUG("Creating sequential reader (BlockCount: %d)", 
        static_cast<int>(blockIndexes.size()));

    for (int i = 0; i < BlockIndexSequence.size(); ++i) {
        BlockWindow.push_back(NewPromise<TSharedRef>());
    }

    ReaderThread->GetInvoker()->Invoke(BIND(
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
    YASSERT(!State.HasRunningOperation());
    YASSERT(NextSequenceIndex > 0);
    YASSERT(BlockWindow[NextSequenceIndex - 1].IsSet());

    return BlockWindow[NextSequenceIndex - 1].Get();
}

TAsyncError TSequentialReader::AsyncNextBlock()
{
    // No thread affinity - can be called from 
    // ContinueNextRow of NTableClient::TChunkReader.

    YASSERT(HasNext());
    YASSERT(!State.HasRunningOperation());

    if (NextSequenceIndex > 0) {
        AsyncSemaphore.Release(BlocksExt.blocks(
            BlockIndexSequence[NextSequenceIndex - 1]).size());
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

    LOG_DEBUG(
        "Got block group (FirstIndex: %d, BlockCount: %d)", 
        firstSequenceIndex, 
        static_cast<int>(readResult.Value().size()));

    int sequenceIndex = firstSequenceIndex;
    FOREACH (auto& block, readResult.Value()) {
        BlockWindow[sequenceIndex].Set(block);
        ++sequenceIndex;
    }

    ReaderThread->GetInvoker()->Invoke(BIND(
        &TSequentialReader::FetchNextGroup,
        MakeWeak(this)));
}

void TSequentialReader::FetchNextGroup()
{
    VERIFY_THREAD_AFFINITY(ReaderThread);

    // ToDo(psushin): maybe use TSmallVector here?
    auto firstUnfetched = NextUnfetchedIndex;
    std::vector<int> blockIndexes;
    int groupSize = 0;
    while (groupSize < Config->GroupSize && NextUnfetchedIndex < BlockIndexSequence.size()) {
        auto blockIndex = BlockIndexSequence[NextUnfetchedIndex];
        blockIndexes.push_back(blockIndex);
        groupSize += BlocksExt.blocks(blockIndex).size();
        ++NextUnfetchedIndex;
    }

    if (!groupSize)
        return;

    LOG_DEBUG(
        "Requesting block group (FirstIndex: %d, BlockCount: %d, GroupSize: %d)", 
        firstUnfetched, 
        static_cast<int>(blockIndexes.size()),
        groupSize);

    AsyncSemaphore.GetReadyEvent().Subscribe(BIND(
        &TSequentialReader::RequestBlocks,
        MakeWeak(this),
        firstUnfetched,
        blockIndexes,
        groupSize));
}

void TSequentialReader::RequestBlocks(
    int firstIndex, 
    const std::vector<int>& blockIndexes,
    int groupSize)
{
    AsyncSemaphore.Acquire(groupSize);
    ChunkReader->AsyncReadBlocks(blockIndexes).Subscribe(BIND(
        &TSequentialReader::OnGotBlocks, 
        MakeWeak(this),
        firstIndex).Via(ReaderThread->GetInvoker()));
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
