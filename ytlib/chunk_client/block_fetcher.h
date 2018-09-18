#pragma once

#include "chunk_reader.h"

#include <yt/client/chunk_client/data_statistics.h>

#include <yt/core/actions/future.h>

#include <yt/core/compression/public.h>

#include <yt/core/concurrency/async_semaphore.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/lazy_ptr.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! For a sequence of block indexes, fetches and uncompresses these blocks in the given order.
/*!
 *  Internally, blocks are prefetched obeying a given memory limit.
 */
class TBlockFetcher
    : public virtual TRefCounted
{
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
        NCompression::ECodec codecId,
        double compressionRatio,
        const TClientBlockReadOptions& blockReadOptions);

    //! Returns |true| if there are requested blocks that were not fetched enough times.
    bool HasMoreBlocks() const;

    //! Asynchronously fetches the block with given index.
    /*!
     *  It is not allowed to fetch the block more times than it has been requested.
     *  If an error occurs during fetching then the whole session is failed.
     */
    TFuture<TBlock> FetchBlock(int blockIndex);

    //! Returns true if all blocks are fetched and false otherwise.
    bool IsFetchingCompleted();

    //! Returns total uncompressed size of read blocks.
    i64 GetUncompressedDataSize() const;

    //! Returns total compressed size of read blocks.
    i64 GetCompressedDataSize() const;

    //! Returns codec and cpu time spent in compression.
    TCodecDuration GetDecompressionTime() const;

private:
    struct TWindowSlot
    {
        TLazyUniquePtr<TPromise<NChunkClient::TBlock>> BlockPromise;
        std::atomic<int> RemainingFetches = { 0 };
        std::unique_ptr<NConcurrency::TAsyncSemaphoreGuard> AsyncSemaphoreGuard;
        bool Cached = false;
        std::atomic_flag FetchStarted = ATOMIC_FLAG_INIT;

        TWindowSlot()
            : BlockPromise(BIND([] () {
            return new TPromise<NChunkClient::TBlock>(NewPromise<NChunkClient::TBlock>());
        }))
        { }
    };

    std::atomic<i64> UncompressedDataSize_ = {0};
    std::atomic<i64> CompressedDataSize_ = {0};

    const TBlockFetcherConfigPtr Config_;
    std::vector<TBlockInfo> BlockInfos_;
    const IChunkReaderPtr ChunkReader_;
    const IBlockCachePtr BlockCache_;

    const IInvokerPtr CompressionInvoker_;
    const double CompressionRatio_;

    THashMap<int, int> BlockIndexToWindowIndex_;

    std::unique_ptr<TWindowSlot[]> Window_;
    int WindowSize_ = 0;

    NConcurrency::TAsyncSemaphorePtr AsyncSemaphore_;

    int TotalRemainingFetches_ = 0;
    std::atomic<i64> TotalRemainingSize_ = { 0 };
    int FirstUnfetchedWindowIndex_ = 0;

    NCompression::ICodec* const Codec_;

    bool IsFetchingCompleted_ = false;

    const TClientBlockReadOptions BlockReadOptions_;

    NLogging::TLogger Logger;

    TDuration DecompressionTime;

    void FetchNextGroup(NConcurrency::TAsyncSemaphoreGuard AsyncSemaphoreGuard);

    void RequestBlocks(
        const std::vector<int>& windowIndexes,
        const std::vector<int>& blockIndexes,
        i64 uncompressedSize);

    void DecompressBlocks(
        const std::vector<int>& windowIndexes,
        const std::vector<NChunkClient::TBlock>& compressedBlocks);

    void MarkFailedBlocks(
        const std::vector<int>& windowIndexes,
        const TError& error);

    void ReleaseBlock(int windowIndex);
};

DEFINE_REFCOUNTED_TYPE(TBlockFetcher)

////////////////////////////////////////////////////////////////////////////////

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
        NCompression::ECodec codecId,
        double compressionRatio,
        const TClientBlockReadOptions& blockReadOptions);

    TFuture<NChunkClient::TBlock> FetchNextBlock();

    using TBlockFetcher::HasMoreBlocks;
    using TBlockFetcher::IsFetchingCompleted;
    using TBlockFetcher::GetUncompressedDataSize;
    using TBlockFetcher::GetCompressedDataSize;
    using TBlockFetcher::GetDecompressionTime;

private:
    std::vector<TBlockInfo> OriginalOrderBlockInfos_;
    int CurrentIndex_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TSequentialBlockFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
