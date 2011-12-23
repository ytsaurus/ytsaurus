#pragma once

#include "common.h"
#include "async_reader.h"
#include "reader_thread.h"

#include "../misc/configurable.h"
#include "../misc/async_stream_state.h"
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

    struct TConfig
        : TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        //! Prefetch window size (in blocks).
        int PrefetchWindowSize;

        //! Maximum number of blocks to be transfered via a single RPC request.
        int GroupSize;

        TConfig()
        {
            Register("prefetch_window_size", PrefetchWindowSize).Default(100).GreaterThan(0);
            Register("group_size", GroupSize).Default(10).GreaterThan(0);
        }

        virtual void Validate(const NYTree::TYPath& path = "") const
        {
            if (GroupSize > PrefetchWindowSize) {
                ythrow yexception() << "\"group_size\" cannot be larger than \"prefetch_window_size\"";
            }

            TConfigurable::Validate(path);
        }
     };

    //! Configures an instance.
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
    TAsyncError::TPtr AsyncNextBlock();

    //! Returns the current block.
    /*!
     *  The block must have been already fetched by #AsyncNextBlock.
     */
    TSharedRef GetBlock();

private:
    struct TWindowSlot
    {
        TFuture<TSharedRef>::TPtr AsyncBlock;

        TWindowSlot()
            : AsyncBlock(New< TFuture<TSharedRef> >())
        { }
    };

    void OnGotBlocks(
        IAsyncReader::TReadResult readResult, 
        int firstSequenceIndex);

    void ShiftWindow();
    void DoShiftWindow();

    void FetchNextGroup();

    const yvector<int> BlockIndexSequence;
    int FirstUnfetchedIndex;

    TConfig::TPtr Config;
    IAsyncReader::TPtr ChunkReader;

    TCyclicBuffer<TWindowSlot> Window;

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
