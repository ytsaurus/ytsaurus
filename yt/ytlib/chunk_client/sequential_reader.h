#pragma once

#include "common.h"
#include "async_reader.h"
#include "reader_thread.h"

#include "../misc/config.h"
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

    bool HasNext() const;

    //! Asynchronously fetches the next block.
    /*!
     *  It is not allowed to ask for the next block until the previous one is retrieved.
     *  If an error occurs during fetching then the whole session is failed.
     */
    TAsyncStreamState::TAsyncResult::TPtr AsyncNextBlock();

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

    const TConfig Config;
    IAsyncReader::TPtr ChunkReader;

    TCyclicBuffer<TWindowSlot> Window;

    //! Number of free slots in window.
    int FreeSlots;

    //! Index in #BlockIndexSequence of next block outputted from #TSequentialChunkReader.
    volatile int NextSequenceIndex;

    TAsyncStreamState State;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
    DECLARE_THREAD_AFFINITY_SLOT(ReaderThread);
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
