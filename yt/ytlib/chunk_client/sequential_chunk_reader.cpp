#include "../misc/stdafx.h"
#include "sequential_chunk_reader.h"

#include "../actions/action_util.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

TLazyPtr<TActionQueue> TSequentialChunkReader::ReaderThread;

///////////////////////////////////////////////////////////////////////////////

TSequentialChunkReader::TSequentialChunkReader(
    const TConfig& config, 
    const yvector<int>& blockIndexes, 
    IChunkReader::TPtr chunkReader)
    : Config(config)
    , BlockIndexSequence(blockIndexes)
    , ChunkReader(chunkReader)
    , Window(config.WindowSize)
    , FreeSlots(config.WindowSize)
{
    VERIFY_THREAD_AFFINITY(Client);
    VERIFY_INVOKER_AFFINITY(ReaderThread->GetInvoker(), Reader);

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
    VERIFY_THREAD_AFFINITY(Client);

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
    VERIFY_THREAD_AFFINITY(Reader);

    if (HasFailed) {
        return;
    }

    if (readResult.IsOK) {
        int sequenceIndex = firstSequenceIndex;
        FOREACH(auto& block, readResult.Blocks) {
            TWindowSlot& slot = GetEmptySlot(sequenceIndex);
            slot.Result.IsOK = true;
            slot.Result.Block = block;
            slot.IsEmpty = false;
        }
    } else {
        TWindowSlot& slot = GetEmptySlot(firstSequenceIndex);
        slot.Result.IsOK = false;
        slot.IsEmpty = false; // Now it can be used from client thread
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
    VERIFY_THREAD_AFFINITY(Reader);

    TWindowSlot& slot = Window.GetFirstSlot();
    YASSERT(!slot.IsEmpty);

    slot.IsEmpty = true;
    slot.Result = TResult();

    Window.Shift();
    ++FreeSlots;
    
    if (FreeSlots > Config.GroupSize) {
        FetchNextGroup();
    }
}

void TSequentialChunkReader::FetchNextGroup()
{
    VERIFY_THREAD_AFFINITY(Reader);

    YASSERT(FreeSlots > Config.GroupSize);

    yvector<int>::const_iterator groupBegin = BlockIndexSequence.begin() + FirstUnfetchedIndex;
    yvector<int>::const_iterator groupEnd = BlockIndexSequence.end();
    if (BlockIndexSequence.size() - FirstUnfetchedIndex > Config.GroupSize) {
        groupEnd = groupBegin + Config.GroupSize;
    }

    if (groupBegin == groupEnd) {
        return;
    }

    yvector<int> groupIndexes(groupBegin, groupEnd);
    ChunkReader->AsyncReadBlocks(groupIndexes)->Subscribe(FromMethod(
        &TSequentialChunkReader::OnGotBlocks, 
        TPtr(this),
        FirstUnfetchedIndex));

    FreeSlots -= groupIndexes.ysize();
    FirstUnfetchedIndex += groupIndexes.ysize();
}

bool TSequentialChunkReader::IsNextSlotEmpty()
{
    VERIFY_THREAD_AFFINITY_ANY();
    TWindowSlot& slot = Window.GetSlot(NextSequenceIndex);
    return slot.IsEmpty;
}

TSequentialChunkReader::TResult& TSequentialChunkReader::GetNextSlotResult()
{
    VERIFY_THREAD_AFFINITY_ANY();
    TWindowSlot& slot = Window.GetSlot(NextSequenceIndex);
    ++NextSequenceIndex;
    YASSERT(!slot.IsEmpty);

    if (!slot.Result.IsOK) {
        HasFailed = true;
    }

    return slot.Result;
}

void TSequentialChunkReader::ProcessPendingResult()
{
    VERIFY_THREAD_AFFINITY(Client);
    ReaderThread->GetInvoker()->Invoke(FromMethod(
        &TSequentialChunkReader::DoProcessPendingResult, 
        TPtr(this)));
}

void TSequentialChunkReader::DoProcessPendingResult()
{
    VERIFY_THREAD_AFFINITY(Reader);
    if (~PendingResult != NULL) {
        return;
    }

    if (!IsNextSlotEmpty()) {
        auto pending = PendingResult;
        PendingResult.Drop();
        pending->Set(GetNextSlotResult());
    }
}

TSequentialChunkReader::TWindowSlot& TSequentialChunkReader::GetEmptySlot(int sequenceIndex)
{
    VERIFY_THREAD_AFFINITY(Reader);
    TWindowSlot& slot = Window.GetSlot(sequenceIndex);
    YASSERT(slot.IsEmpty);
    return slot;
}

// ToDo: consider removing?
IInvoker::TPtr TSequentialChunkReader::GetInvoker()
{
    return ReaderThread->GetInvoker();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT

