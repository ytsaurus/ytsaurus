#include "stdafx.h"
#include "sequential_reader.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkClientLogger;

///////////////////////////////////////////////////////////////////////////////

TSequentialReader::TSequentialReader(
    TConfig* config, 
    const yvector<int>& blockIndexes, 
    IAsyncReader* chunkReader)
    : BlockIndexSequence(blockIndexes)
    , FirstUnfetchedIndex(0)
    , Config(config)
    , ChunkReader(chunkReader)
    , Window(config->PrefetchWindowSize)
    , FreeSlots(config->PrefetchWindowSize)
    , NextSequenceIndex(0)
{
    VERIFY_INVOKER_AFFINITY(ReaderThread->GetInvoker(), ReaderThread);

    YASSERT(ChunkReader);
    YASSERT(blockIndexes.ysize() > 0);
    YASSERT(Config->GroupSize <= Config->PrefetchWindowSize);

    LOG_DEBUG("Creating sequential reader (blockCount: %d)", blockIndexes.ysize());

    int fetchCount = FreeSlots / Config->GroupSize;
    for (int i = 0; i < fetchCount; ++i) {
        ReaderThread->GetInvoker()->Invoke(BIND(
            &TSequentialReader::FetchNextGroup,
            MakeWeak(this)));
    }
}

bool TSequentialReader::HasNext() const
{
    // No thread affinity - can be called from 
    // ContinueNextRow of NTableClient::TChunkReader.
    return NextSequenceIndex < BlockIndexSequence.ysize();
}

TSharedRef TSequentialReader::GetBlock()
{
    // No thread affinity - can be called from 
    // ContinueNextRow of NTableClient::TChunkReader.

    YASSERT(!State.HasRunningOperation());
    YASSERT(NextSequenceIndex > 0);
    return Window[NextSequenceIndex - 1].AsyncBlock->Get();
}

TAsyncError TSequentialReader::AsyncNextBlock()
{
    // No thread affinity - can be called from 
    // ContinueNextRow of NTableClient::TChunkReader.

    YASSERT(HasNext());
    YASSERT(!State.HasRunningOperation());

    State.StartOperation();

    auto this_ = MakeStrong(this);
    Window[NextSequenceIndex].AsyncBlock->Subscribe(
        BIND([=] (TSharedRef) {
            this_->State.FinishOperation();
        }));

    if (NextSequenceIndex > 0) {
        ShiftWindow();
    }

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
        "Got block group (firtsIndex: %d, blockCount: %d)", 
        firstSequenceIndex, 
        readResult.Value().ysize());

    int sequenceIndex = firstSequenceIndex;
    FOREACH (auto& block, readResult.Value()) {
        Window[sequenceIndex].AsyncBlock->Set(block);
        ++sequenceIndex;
    }
}

void TSequentialReader::ShiftWindow()
{
    VERIFY_THREAD_AFFINITY_ANY();

    ReaderThread->GetInvoker()->Invoke(BIND(
        &TSequentialReader::DoShiftWindow, 
        MakeWeak(this)));
}

void TSequentialReader::DoShiftWindow()
{
    VERIFY_THREAD_AFFINITY(ReaderThread);

    LOG_DEBUG("Window shifted");

    Window.Shift();
    ++FreeSlots;
    
    if (FreeSlots >= Config->GroupSize || 
        // Fetch the last group as soon as we can.
        BlockIndexSequence.ysize() - FirstUnfetchedIndex <= FreeSlots) 
    {
        FetchNextGroup();
    }
}

void TSequentialReader::FetchNextGroup()
{
    VERIFY_THREAD_AFFINITY(ReaderThread);

    auto groupBegin = BlockIndexSequence.begin() + FirstUnfetchedIndex;
    auto groupEnd = BlockIndexSequence.end();
    if (BlockIndexSequence.ysize() - FirstUnfetchedIndex > Config->GroupSize) {
        groupEnd = groupBegin + Config->GroupSize;
    }

    if (groupBegin == groupEnd) {
        return;
    }

    yvector<int> groupIndexes(groupBegin, groupEnd);
    LOG_DEBUG(
        "Requesting block group (firstIndex: %d, blockCount: %d)", 
        FirstUnfetchedIndex, 
        groupIndexes.ysize());

    ChunkReader->AsyncReadBlocks(groupIndexes)->Subscribe(BIND(
        &TSequentialReader::OnGotBlocks, 
        MakeWeak(this),
        FirstUnfetchedIndex)
            .Via(ReaderThread->GetInvoker()));

    FreeSlots -= groupIndexes.ysize();
    FirstUnfetchedIndex += groupIndexes.ysize();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
