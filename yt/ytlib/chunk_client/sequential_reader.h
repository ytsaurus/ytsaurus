#pragma once

#include "public.h"
#include "reader.h"

#include <core/actions/future.h>

#include <core/misc/ref.h>
#include <core/misc/async_stream_state.h>
#include <core/misc/property.h>

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/async_semaphore.h>

#include <core/compression/public.h>

#include <ytlib/chunk_client/chunk_meta.pb.h>

#include <core/logging/tagged_logger.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! For a sequence of block indexes fetches and outputs these blocks in the given order.
//! Prefetches and stores a configured number of blocks in its internal cyclic buffer.
class TSequentialReader
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(volatile i64, UncompressedDataSize);
    DEFINE_BYVAL_RO_PROPERTY(volatile i64, CompressedDataSize);

public:
    struct TBlockInfo
    {
        int Index;
        int Size;

        TBlockInfo()
            : Index(-1)
            , Size(0)
        { }

        TBlockInfo(int index, int size)
            : Index(index)
            , Size(size)
        { }
    };

    TSequentialReader(
        TSequentialReaderConfigPtr config,
        std::vector<TBlockInfo> blockInfos,
        IReaderPtr chunkReader,
        NCompression::ECodec codecId);

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

    TFuture<void> GetFetchingCompleteEvent();

private:
    void OnGotBlocks(
        int firstSequenceIndex,
        IReader::TReadBlocksResult readResult);

    void FetchNextGroup();
    void RequestBlocks(
        int firstIndex,
        const std::vector<int>& blockIndexes,
        i64 groupSize);

    void DecompressBlocks(
        int blockIndex,
        const IReader::TReadBlocksResult& readResult);

    const std::vector<TBlockInfo> BlockInfos_;

    TSequentialReaderConfigPtr Config_;
    IReaderPtr ChunkReader_;

    std::vector< TPromise<TSharedRef> > BlockWindow;

    NConcurrency::TAsyncSemaphore AsyncSemaphore_;

    //! Index in #BlockIndexSequence of next block outputted from #TSequentialChunkReader.
    volatile int NextSequenceIndex = 0;
    int NextUnfetchedIndex = 0;
 
    TPromise<void> FetchingCompleteEvent = NewPromise();

    TAsyncStreamState State;
    NCompression::ICodec* Codec_;

    NLog::TTaggedLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(ReaderThread);

};

DEFINE_REFCOUNTED_TYPE(TSequentialReader)

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
