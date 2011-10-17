#pragma once

#include "common.h"
#include "chunk_reader.h"

#include "../misc/ptr.h"
#include "../misc/lazy_ptr.h"
#include "../misc/enum.h"
#include "../misc/thread_affinity.h"
#include "../actions/future.h"
#include "../actions/action_queue.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

//! Thread safe cyclic buffer
template <class T>
class TCyclicBuffer
{
public:
    TCyclicBuffer(int size)
        : Window(size)
    { }

    T& GetSlot(int index)
    {
        TGuard<TSpinLock> guard(SpinLock);
        YASSERT(StartIndex <= index);
        YASSERT(index < StartIndex + Window.size());

        return Window[Start + (index - StartIndex)];
    }

    const T& GetSlot(int index) const
    {
        TGuard<TSpinLock> guard(SpinLock);
        YASSERT(StartIndex <= index);
        YASSERT(index < StartIndex + Window.size());

        return Window[Start + (index - StartIndex)];
    }

    T& GetFirstSlot()
    {
        return GetSlot(StartIndex);
    }

    const T& GetFirstSlot() const
    {
        return GetSlot(StartIndex);
    }

    void Shift()
    {
        TGuard<TSpinLock> guard(SpinLock);
        Start = (++Start) % Window.size();
        ++StartIndex;
    }

private:
    autoarray<T> Window;
    int Start;
    int StartIndex;

    TSpinLock SpinLock;
};

///////////////////////////////////////////////////////////////////////////////

//! Given a sequence of the block indexes can output blocks
//! one-by-one via async interface with prefetching.
class TSequentialChunkReader
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSequentialChunkReader> TPtr;

    struct TResult
    {
        bool IsOK;
        TSharedRef Block;
    };

    struct TConfig
    {
        int WindowSize;
        int GroupSize;
    };

    TSequentialChunkReader(
        const TConfig& config,
        const yvector<int>& blockIndexes,
        IChunkReader::TPtr chunkReader);

    /*!
     *  Client is not allowed to ask for the next block, 
     *  until previous one is set. If TResult.IsOK is false,
     *  then the whole session failed and following calls to
     *  #AsyncGetNextBlock are forbidden.
     */
    TFuture<TResult>::TPtr AsyncGetNextBlock();

    static IInvoker::TPtr GetInvoker();

private:
    struct TWindowSlot
    {
        bool IsEmpty;
        TResult Result;
    };

    void TSequentialChunkReader::OnGotBlocks(
        IChunkReader::TReadResult readResult, 
        int firstSequenceIndex);

    void ProcessPendingResult();
    void DoProcessPendingResult();

    void ShiftWindow();
    void DoShiftWindow();

    void FetchNextGroup();

    bool IsNextSlotEmpty();
    TResult& GetNextSlotResult();

    TWindowSlot& GetEmptySlot(int sequenceIndex);

    const yvector<int> BlockIndexSequence;
    int FirstUnfetchedIndex;

    const TConfig Config;
    IChunkReader::TPtr ChunkReader;

    TCyclicBuffer<TWindowSlot> Window;

    //! Number of free slots in window
    int FreeSlots;

    //! Block, that has been already requested by client,
    //! but not delivered from holder yet
    TFuture<TResult>::TPtr PendingResult;

    bool HasFailed;

    //! Index in #BlockIndexSequence of next block outputted from #TSequentialChunkReader
    int NextSequenceIndex;

    DECLARE_THREAD_AFFINITY_SLOT(Client);
    DECLARE_THREAD_AFFINITY_SLOT(Reader);

    static TLazyPtr<TActionQueue> ReaderThread;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
