#pragma once

#include "public.h"
#include "async_reader.h"

#include <ytlib/chunk_holder/chunk.pb.h>
#include <ytlib/actions/future.h>

#include <ytlib/misc/async_stream_state.h>
#include <ytlib/misc/semaphore.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/ref.h>

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

    TSequentialReader(
        TSequentialReaderConfigPtr config,
        // ToDo: use move semantics
        const std::vector<int>& blockIndexes,
        IAsyncReaderPtr chunkReader,
        // ToDo: use move semantics
        TAutoPtr<NChunkHolder::NProto::TBlocks> protoBlocks);

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

    void FetchNextGroup();
    void RequestBlocks(
        int firstIndex, 
        const std::vector<int>& blockIndexes,
        TVoid);

    const std::vector<int> BlockIndexSequence;
    TAutoPtr<NChunkHolder::NProto::TBlocks> ProtoBlocks;

    TSequentialReaderConfigPtr Config;
    IAsyncReaderPtr ChunkReader;

    std::vector< TPromise<TSharedRef> > BlockWindow;

    TAsyncSemaphore AsyncSemaphore;

    //! Index in #BlockIndexSequence of next block outputted from #TSequentialChunkReader.
    volatile int NextSequenceIndex;
    int NextUnfetchedIndex;

    TAsyncStreamState State;

    DECLARE_THREAD_AFFINITY_SLOT(ReaderThread);
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
