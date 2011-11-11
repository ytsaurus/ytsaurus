#include "stdafx.h"
#include "sequential_chunk_reader.h"

#include "../actions/action_util.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

TSequentialChunkReader::TSequentialChunkReader(
    const TConfig& config, 
    const yvector<int>& blockIndexes, 
    IChunkReader::TPtr chunkReader)
    : BlockIndexSequence(blockIndexes)
    , FirstUnfetchedIndex(0)
    , Config(config)
    , ChunkReader(chunkReader)
    , Window(config.WindowSize)
    , FreeSlots(config.WindowSize)
    , PendingResult(NULL)
    , HasFailed(false)
    , NextSequenceIndex(0)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    VERIFY_INVOKER_AFFINITY(ReaderThread->GetInvoker(), ReaderThread);

    YASSERT(~ChunkReader != NULL);
    YASSERT(blockIndexes.ysize() > 0);
    YASSERT(Config.GroupSize <= Config.WindowSize);

    int fetchCount = FreeSlots / Config.GroupSize;
    for (int i = 0; i < fetchCount; ++i) {
        ReaderThread->GetInvoker()->Invoke(FromMethod(
            &TSequentialChunkReader::FetchNextGroup,
            TPtr(this)));
    }
}

TFuture<TSequentialChunkReader::TResult>::TPtr
TSequentialChunkReader::AsyncGetNextBlock()
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(~PendingResult == NULL);
    YASSERT(!HasFailed);

    auto result = New< TFuture<TResult> >();
    if (IsNextSlotEmpty()) {
        PendingResult = result;
        ProcessPendingResult();
    } else {
        result->Set(GetNextSlotResult());
        ShiftWindow();
    }

    return result;
}

void TSequentialChunkReader::OnGotBlocks(
    IChunkReader::TReadResult readResult, 
    int firstSequenceIndex)
{
    VERIFY_THREAD_AFFINITY(ReaderThread);

    if (HasFailed) {
        return;
    }

    if (readResult.IsOK) {
        int sequenceIndex = firstSequenceIndex;
        FOREACH(auto& block, readResult.Blocks) {
            auto& slot = GetEmptySlot(sequenceIndex);
            slot.Result.IsOK = true;
            slot.Result.Block = block;
            slot.IsEmpty = false; // Now slot can be used from client thread
            ++sequenceIndex;
        }
    } else {
        auto& slot = GetEmptySlot(firstSequenceIndex);
        slot.Result.IsOK = false;
        slot.IsEmpty = false; // Now slot can be used from client thread
    }

    DoProcessPendingResult();
}

void TSequentialChunkReader::ShiftWindow()
{
    VERIFY_THREAD_AFFINITY_ANY();

    ReaderThread->GetInvoker()->Invoke(FromMethod(
        &TSequentialChunkReader::DoShiftWindow, 
        TPtr(this)));
}

void TSequentialChunkReader::DoShiftWindow()
{
    VERIFY_THREAD_AFFINITY(ReaderThread);

    auto& slot = Window.First();
    YASSERT(!slot.IsEmpty);

    slot.IsEmpty = true;
    slot.Result = TResult();

    Window.Shift();
    ++FreeSlots;
    
    if (FreeSlots >= Config.GroupSize || 
        // Fetch the last group as soon as we can.
        BlockIndexSequence.ysize() - FirstUnfetchedIndex <= FreeSlots) 
    {
        FetchNextGroup();
    }
}

void TSequentialChunkReader::FetchNextGroup()
{
    VERIFY_THREAD_AFFINITY(ReaderThread);
    YASSERT(FreeSlots >= Config.GroupSize);

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
        &TSequentialChunkReader::OnGotBlocks, 
        TPtr(this),
        FirstUnfetchedIndex)
            ->Via(ReaderThread->GetInvoker()));

    FreeSlots -= groupIndexes.ysize();
    FirstUnfetchedIndex += groupIndexes.ysize();
}

bool TSequentialChunkReader::IsNextSlotEmpty()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YASSERT(NextSequenceIndex < BlockIndexSequence.ysize());
    auto& slot = Window[NextSequenceIndex];
    return slot.IsEmpty;
}

TSequentialChunkReader::TResult& TSequentialChunkReader::GetNextSlotResult()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YASSERT(NextSequenceIndex < BlockIndexSequence.ysize());
    auto& slot = Window[NextSequenceIndex];
    ++NextSequenceIndex;
    YASSERT(!slot.IsEmpty);

    if (!slot.Result.IsOK) {
        HasFailed = true;
    }

    return slot.Result;
}

void TSequentialChunkReader::ProcessPendingResult()
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    
    ReaderThread->GetInvoker()->Invoke(FromMethod(
        &TSequentialChunkReader::DoProcessPendingResult, 
        TPtr(this)));
}

void TSequentialChunkReader::DoProcessPendingResult()
{
    VERIFY_THREAD_AFFINITY(ReaderThread);
    
    if (~PendingResult == NULL) {
        return;
    }

    if (!IsNextSlotEmpty()) {
        auto pending = PendingResult;
        PendingResult.Reset();
        pending->Set(GetNextSlotResult());
    }
}

TSequentialChunkReader::TWindowSlot& TSequentialChunkReader::GetEmptySlot(int sequenceIndex)
{
    VERIFY_THREAD_AFFINITY(ReaderThread);

    auto& slot = Window[sequenceIndex];
    YASSERT(slot.IsEmpty);
    return slot;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT

