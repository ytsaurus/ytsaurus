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

#include <core/logging/log.h>

#include <ytlib/chunk_client/chunk_meta.pb.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! For a sequence of block indexes, fetches and uncompresses these blocks in the given order.
/*!
 *  Internally, blocks are prefetched obeying a given memory limit.
 */
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
        int UncompressedDataSize;

        TBlockInfo()
            : Index(-1)
            , UncompressedDataSize(0)
        { }

        TBlockInfo(int index, int uncompressedDataSize)
            : Index(index)
            , UncompressedDataSize(uncompressedDataSize)
        { }
    };

    TSequentialReader(
        TSequentialReaderConfigPtr config,
        std::vector<TBlockInfo> blockInfos,
        IChunkReaderPtr chunkReader,
        IBlockCachePtr uncompressedBlockCache,
        NCompression::ECodec codecId);

    //! Returns |true| if the current block is not the last one.
    bool HasMoreBlocks() const;

    //! Asynchronously fetches the next block.
    /*!
     *  It is not allowed to ask for the next block until the previous one is retrieved.
     *  If an error occurs during fetching then the whole session is failed.
     */
    TAsyncError FetchNextBlock();

    //! Returns the current block.
    /*!
     *  The block must have been already fetched by #FetchNextBlock.
     */
    TSharedRef GetCurrentBlock();

    //! Returns a asynchronous flag that becomes set when all
    //! blocks are fetched.
    TFuture<void> GetFetchingCompleteEvent();

private:
    void FetchNextGroup();

    void RequestBlocks(
        const std::vector<int>& windowIndexes,
        const std::vector<int>& blockIndexes,
        i64 uncompressedSize);

    void OnGotBlocks(
        const std::vector<int>& windowIndexes,
        const std::vector<int>& blockIndexes,
        IChunkReader::TReadBlocksResult readResult);

    void DecompressBlocks(
        const std::vector<int>& windowIndexes,
        const std::vector<TSharedRef>& compressedBlocks);


    TSequentialReaderConfigPtr Config_;
    std::vector<TBlockInfo> BlockInfos_;
    IChunkReaderPtr ChunkReader_;
    IBlockCachePtr UncompressedBlockCache_;

    struct TWindowSlot
    {
        TPromise<TSharedRef> Block = NewPromise<TSharedRef>();
        bool Cached = false;
    };

    std::vector<TWindowSlot> Window_;

    NConcurrency::TAsyncSemaphore AsyncSemaphore_;

    volatile int FirstReadyWindowIndex_ = -1;
    int FirstUnfetchedWindowIndex_ = 0;
 
    TPromise<void> FetchingComplete_ = NewPromise<void>();

    TAsyncStreamState State_;
    NCompression::ICodec* Codec_;

    NLog::TLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(ReaderThread);

};

DEFINE_REFCOUNTED_TYPE(TSequentialReader)

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
