#pragma once

#include "common.h"
#include "async_reader.h"
#include "reader_thread.h"

#include "../misc/config.h"
#include "../misc/enum.h"
#include "../misc/cyclic_buffer.h"
#include "../misc/thread_affinity.h"
#include "../actions/future.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! For a sequence of block indexes fetches and outputs these blocks in the given order.
//! Prefetches and stores a configured number of blocks in its internal cyclic buffer.
class TSequentialReader
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSequentialReader> TPtr;

    // ToDo: use TAsyncError and TAsyncStreamState
    struct TResult
    {
        bool IsOK;
        TSharedRef Block;
    };

    struct TConfig
        : TConfigBase
    {
        //! Prefetch window size (in blocks).
        int PrefetchWindowSize;

        //! Maximum number of blocks to be transfered via a single RPC request.
        int GroupSize;

        TConfig()
        {
            Register("window_size", PrefetchWindowSize).Default(40).GreaterThan(0);
            Register("group_size", GroupSize).Default(8).GreaterThan(0);

            SetDefaults();
        }

        virtual void Validate(const Stroka& path = "") const
        {
            if (GroupSize > PrefetchWindowSize) {
                ythrow yexception() << "Group size cannot be larger than prefetch window size";
            }

            TConfigBase::Validate(path);
        }
     };

    //! Configures an instance.
    TSequentialReader(
        const TConfig& config,
        const yvector<int>& blockIndexes,
        IAsyncReader* chunkReader);

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
        IAsyncReader::TReadResult readResult, 
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
    IAsyncReader::TPtr ChunkReader;

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

} // namespace NChunkClient
} // namespace NYT
