#pragma once

#include "public.h"
#include "async_reader.h"

#include <ytlib/chunk_holder/chunk.pb.h>
#include <ytlib/actions/future.h>

#include <ytlib/misc/async_stream_state.h>
#include <ytlib/misc/semaphore.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/codec.h>
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

    struct TBlockInfo
    {
        int Index;
        int Size;

        TBlockInfo(int index, int size)
            : Index(Index)
            , Size(size)
        { }
    };

    TSequentialReader(
        TSequentialReaderConfigPtr config,
        // ToDo: use move semantics
        std::vector<TBlockInfo>&& blocks,
        IAsyncReaderPtr chunkReader,
        ECodecId codecId);

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
        int groupSize);

    void DecompressBlock(
        int firstSequenceIndex,
        int blockIndex,
        const IAsyncReader::TReadResult& readResult);

    const std::vector<TBlockInfo> BlockSequence;

    TSequentialReaderConfigPtr Config;
    IAsyncReaderPtr ChunkReader;

    std::vector< TPromise<TSharedRef> > BlockWindow;

    TAsyncSemaphore AsyncSemaphore;

    //! Index in #BlockIndexSequence of next block outputted from #TSequentialChunkReader.
    volatile int NextSequenceIndex;
    int NextUnfetchedIndex;

    TAsyncStreamState State;
    ICodec* Codec;

    DECLARE_THREAD_AFFINITY_SLOT(ReaderThread);
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
