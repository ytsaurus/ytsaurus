#include "stdafx.h"
#include "sequential_reader.h"

#include "../actions/action_util.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

TSequentialReader::TSequentialReader(
    const TConfig& config, 
    const yvector<int>& blockIndexes, 
    IAsyncReader* chunkReader)
    : BlockIndexSequence(blockIndexes)
    , FirstUnfetchedIndex(0)
    , Config(config)
    , ChunkReader(chunkReader)
    , Window(config.PrefetchWindowSize)
    , FreeSlots(config.PrefetchWindowSize)
    , NextSequenceIndex(0)
{
    VERIFY_INVOKER_AFFINITY(ReaderThread->GetInvoker(), ReaderThread);

    YASSERT(~ChunkReader != NULL);
    YASSERT(blockIndexes.ysize() > 0);
    YASSERT(Config.GroupSize <= Config.PrefetchWindowSize);

    int fetchCount = FreeSlots / Config.GroupSize;
    for (int i = 0; i < fetchCount; ++i) {
        ReaderThread->GetInvoker()->Invoke(FromMethod(
            &TSequentialReader::FetchNextGroup,
            TPtr(this)));
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

TAsyncStreamState::TAsyncResult::TPtr
TSequentialReader::AsyncNextBlock()
{
    // No thread affinity - can be called from 
    // ContinueNextRow of NTableClient::TChunkReader.

    YASSERT(HasNext());
    YASSERT(!State.HasRunningOperation());

    State.StartOperation();

    Window[NextSequenceIndex].AsyncBlock->Subscribe(FromMethod(
        &TAsyncStreamState::FinishOperation,
        &State,
        TAsyncStreamState::TResult())->ToParamAction<TSharedRef>());

    if (NextSequenceIndex > 0)
        ShiftWindow();

    ++NextSequenceIndex;

    return State.GetOperationResult();
}
void TSequentialReader::OnGotBlocks(
    IAsyncReader::TReadResult readResult, 
    int firstSequenceIndex)
{
    VERIFY_THREAD_AFFINITY(ReaderThread);

    if (!State.IsActive())
        return;

    if (!readResult.IsOK()) {
        State.Fail(readResult.ToString());
        return;
    }

    int sequenceIndex = firstSequenceIndex;
    FOREACH(auto& block, readResult.Value()) {
        Window[sequenceIndex].AsyncBlock->Set(block);
        ++sequenceIndex;
    }
}

void TSequentialReader::ShiftWindow()
{
    VERIFY_THREAD_AFFINITY_ANY();

    ReaderThread->GetInvoker()->Invoke(FromMethod(
        &TSequentialReader::DoShiftWindow, 
        TPtr(this)));
}

void TSequentialReader::DoShiftWindow()
{
    VERIFY_THREAD_AFFINITY(ReaderThread);

    Window.Shift();
    ++FreeSlots;
    
    if (FreeSlots >= Config.GroupSize || 
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
    if (BlockIndexSequence.ysize() - FirstUnfetchedIndex > Config.GroupSize) {
        groupEnd = groupBegin + Config.GroupSize;
    }

    if (groupBegin == groupEnd) {
        return;
    }

    yvector<int> groupIndexes(groupBegin, groupEnd);
    ChunkReader->AsyncReadBlocks(groupIndexes)->Subscribe(FromMethod(
        &TSequentialReader::OnGotBlocks, 
        TPtr(this),
        FirstUnfetchedIndex)
            ->Via(ReaderThread->GetInvoker()));

    FreeSlots -= groupIndexes.ysize();
    FirstUnfetchedIndex += groupIndexes.ysize();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
