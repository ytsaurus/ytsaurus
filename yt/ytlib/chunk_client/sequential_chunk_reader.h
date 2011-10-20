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

// ToDo: move to misc
//! Thread safe cyclic buffer
template <class T>
class TCyclicBuffer
{
public:
    TCyclicBuffer(int size)
        : Window(size)
        , CyclicStart(0)
        , WindowStart(0)
    { }

    T& operator[](int index)
    {
        TGuard<TSpinLock> guard(SpinLock);
        YASSERT(WindowStart <= index);
        YASSERT(index < WindowStart + Window.size());

        return Window[(CyclicStart + (index - WindowStart)) % Window.size()];
    }

    const T& operator[](int index) const
    {
        TGuard<TSpinLock> guard(SpinLock);
        YASSERT(WindowStart <= index);
        YASSERT(index < WindowStart + Window.size());

        return Window[CyclicStart + (index - WindowStart)];
    }

    T& First()
    {
        return Window[CyclicStart];
    }

    const T& First() const
    {
        return Window[CyclicStart];
    }

    void Shift()
    {
        TGuard<TSpinLock> guard(SpinLock);
        CyclicStart = (++CyclicStart) % Window.size();
        ++WindowStart;
    }

private:
    autoarray<T> Window;

    //! Current index of the first slot in #Window
    int CyclicStart;

    int WindowStart;

    TSpinLock SpinLock;
};

///////////////////////////////////////////////////////////////////////////////

//! Given a sequence of block indexes outputs blocks
//! one-by-one via async interface. Prefetches and stores 
//! limited number of blocks in the internal cyclic buffer.
class TSequentialChunkReader
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSequentialChunkReader> TPtr;

    struct TResult
    {
        //ToDo: proper error code
        bool IsOK;
        TSharedRef Block;
    };

    struct TConfig
    {
        //! Size of prefetching buffer in blocks
        int WindowSize;

        //! Maximum number of blocks for one rpc request
        int GroupSize;

        // ToDo: read from config
        TConfig()
            : WindowSize(40)
            , GroupSize(8)
        { }
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

private:
    struct TWindowSlot
    {
        bool IsEmpty;
        TResult Result;

        TWindowSlot()
            : IsEmpty(true)
        { }
    };

    void OnGotBlocks(
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

    //! Number of free slots in window.
    int FreeSlots;

    //! Block, that has been already requested by client,
    //! but not delivered from holder yet.
    TFuture<TResult>::TPtr PendingResult;

    bool HasFailed;

    //! Index in #BlockIndexSequence of next block outputted from #TSequentialChunkReader.
    int NextSequenceIndex;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
    DECLARE_THREAD_AFFINITY_SLOT(ReaderThread);

    static TLazyPtr<TActionQueue> ReaderThread;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
