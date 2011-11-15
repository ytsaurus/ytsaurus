#pragma once

#include "common.h"
#include "chunk_reader.h"
#include "reader_thread.h"

#include "../misc/common.h"
#include "../misc/enum.h"
#include "../misc/cyclic_buffer.h"
#include "../misc/thread_affinity.h"
#include "../actions/future.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

//! For a sequence of block indexes fetches and outputs these blocks in the given order.
//! Prefetches and stores a configured number of blocks in its internal cyclic buffer.
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
        //! Prefetch window size (in blocks).
        int WindowSize;

        //! Maximum number of blocks to be transfered via a single RPC request.
        int GroupSize;

        // ToDo: read from config
        TConfig()
            : WindowSize(40)
            , GroupSize(8)
        { }
    };

    //! Configures an instance.
    TSequentialChunkReader(
        const TConfig& config,
        const yvector<int>& blockIndexes,
        IChunkReader::TPtr chunkReader);

    //! Asynchronously fetches the next block.
    /*!
     *  It is not allowed to ask for the next block until the previous one is retrieved.
     *  If an error occurs during fetching (which is indicated by TResult::IsOK set to false)
     *  then the whole session is failed and no further calls to #AsyncGetNextBlock are allowed.
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
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
