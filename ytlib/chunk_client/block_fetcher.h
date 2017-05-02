#pragma once

#include "public.h"
#include "chunk_reader.h"

#include <yt/core/actions/future.h>

#include <yt/core/compression/public.h>

#include <yt/core/concurrency/async_semaphore.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! For a sequence of block indexes, fetches and uncompresses these blocks in the given order.
/*!
 *  Internally, blocks are prefetched obeying a given memory limit.
 */
class TBlockFetcher
    : public virtual TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(i64, UncompressedDataSize);
    DEFINE_BYVAL_RO_PROPERTY(i64, CompressedDataSize);

public:
    struct TBlockInfo
    {
        int Index;
        int UncompressedDataSize;
        int Priority;

        TBlockInfo()
            : Index(-1)
            , UncompressedDataSize(0)
            , Priority(0)
        { }

        TBlockInfo(int index, int uncompressedDataSize, int priority)
            : Index(index)
            , UncompressedDataSize(uncompressedDataSize)
            , Priority(priority)
        { }
    };

    TBlockFetcher(
        TBlockFetcherConfigPtr config,
        std::vector<TBlockInfo> blockInfos,
        NConcurrency::TAsyncSemaphorePtr asyncSemaphore,
        IChunkReaderPtr chunkReader,
        IBlockCachePtr blockCache,
        NCompression::ECodec codecId);

    //! Returns |true| if there are requested blocks that were not fetched enough times.
    bool HasMoreBlocks() const;

    //! Asynchronously fetches the block with given index.
    /*!
     *  It is not allowed to fetch the block more times than it has been requested.
     *  If an error occurs during fetching then the whole session is failed.
     */
    TFuture<TSharedRef> FetchBlock(int blockIndex);

    //! Returns true if all blocks are fetched and false otherwise.
    bool IsFetchingCompleted();

private:
    void FetchNextGroup(NConcurrency::TAsyncSemaphoreGuard AsyncSemaphoreGuard);

    void RequestBlocks(
        const std::vector<int>& windowIndexes,
        const std::vector<int>& blockIndexes,
        i64 uncompressedSize);

    void DecompressBlocks(
        const std::vector<int>& windowIndexes,
        const std::vector<TSharedRef>& compressedBlocks);

    void MarkFailedBlocks(
        const std::vector<int>& windowIndexes,
        const TError& error);

    void ReleaseBlock(int windowIndex);

    const TBlockFetcherConfigPtr Config_;
    std::vector<TBlockInfo> BlockInfos_;
    const IChunkReaderPtr ChunkReader_;
    const IBlockCachePtr BlockCache_;

    const IInvokerPtr CompressionInvoker_;

    struct TWindowSlot
    {
        TPromise<TSharedRef> Block = NewPromise<TSharedRef>();
        std::atomic<int> RemainingFetches = { 0 };
        std::unique_ptr<NConcurrency::TAsyncSemaphoreGuard> AsyncSemaphoreGuard;
        bool Cached = false;
        std::atomic_flag FetchStarted = ATOMIC_FLAG_INIT;
    };

    yhash<int, int> BlockIndexToWindowIndex_;

    std::unique_ptr<TWindowSlot[]> Window_;
    int WindowSize_ = 0;

    NConcurrency::TAsyncSemaphorePtr AsyncSemaphore_;

    int TotalRemainingFetches_ = 0;
    std::atomic<i64> TotalRemainingSize_ = { 0 };
    int FirstUnfetchedWindowIndex_ = 0; 

    NCompression::ICodec* const Codec_;

    bool IsFetchingCompleted_ = false;
    
    NLogging::TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TBlockFetcher)

//////////////////////////////////////////////////////////////////////////////

class TSequentialBlockFetcher
    : public virtual TRefCounted
    , private TBlockFetcher
{
public:
    TSequentialBlockFetcher(
        TBlockFetcherConfigPtr config,
        std::vector<TBlockInfo> blockInfos,
        NConcurrency::TAsyncSemaphorePtr asyncSemaphore,
        IChunkReaderPtr chunkReader,
        IBlockCachePtr blockCache,
        NCompression::ECodec codecId);

    TFuture<TSharedRef> FetchNextBlock(); 

    using TBlockFetcher::HasMoreBlocks;
    using TBlockFetcher::IsFetchingCompleted;
    using TBlockFetcher::GetUncompressedDataSize;
    using TBlockFetcher::GetCompressedDataSize;

private:
    std::vector<TBlockInfo> OriginalOrderBlockInfos_;
    int CurrentIndex_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TSequentialBlockFetcher)

//////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
