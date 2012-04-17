#pragma once

#include "common.h"
#include "async_reader.h"
#include "reader_thread.h"

#include <ytlib/misc/configurable.h>
#include <ytlib/misc/async_stream_state.h>
#include <ytlib/misc/enum.h>
#include <ytlib/misc/cyclic_buffer.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/actions/future.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! For a sequence of block indexes fetches and outputs these blocks in the given order.
//! Prefetches and stores a configured number of blocks in its internal cyclic buffer.
class TSequentialReader
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TSequentialReader> TPtr;

    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        //! Prefetch window size (in blocks).
        int PrefetchWindowSize;

        //! Maximum number of blocks to be transfered via a single RPC request.
        int GroupSize;

        TConfig()
        {
            Register("prefetch_window_size", PrefetchWindowSize)
                .Default(100)
                .GreaterThan(0);
            Register("group_size", GroupSize)
                .Default(10)
                .GreaterThan(0);
        }

        virtual void DoValidate() const
        {
            if (GroupSize > PrefetchWindowSize) {
                ythrow yexception() << "\"group_size\" cannot be larger than \"prefetch_window_size\"";
            }
        }
     };

    TSequentialReader(
        TConfig* config,
        const yvector<int>& blockIndexes,
        IAsyncReader* chunkReader);

    bool HasNext() const;

    //! Asynchronously fetches the next block.
    /*!
     *  It is not allowed to ask for the next block until the previous one is retrieved.
     *  If an error occurs during fetching then the whole session is failed.
     */
    TAsyncError AsyncNextBlock();

    //! Returns the current block.
    /*!
     *  The block must have been already fetched by #AsyncNextBlock.
     */
    TSharedRef GetBlock();

private:
    void OnGotBlocks(
        int firstSequenceIndex,
        IAsyncReader::TReadResult readResult);

    void ShiftWindow();
    void DoShiftWindow();

    void FetchNextGroup();

    const yvector<int> BlockIndexSequence;
    int FirstUnfetchedIndex;

    TConfig::TPtr Config;
    IAsyncReader::TPtr ChunkReader;

    struct TWindowSlot
    {
        TWindowSlot()
            : Promise(NewPromise<TSharedRef>())
        { }

        TPromise<TSharedRef> Promise;
    };
    typedef TCyclicBuffer<TWindowSlot> TWindow;

    TWindow Window;

    //! Number of free slots in window.
    int FreeSlots;

    //! Index in #BlockIndexSequence of next block outputted from #TSequentialChunkReader.
    volatile int NextSequenceIndex;

    TAsyncStreamState State;

    DECLARE_THREAD_AFFINITY_SLOT(ReaderThread);
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
