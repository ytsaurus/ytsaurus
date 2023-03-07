#pragma once

#include "chunk_reader.h"
#include "chunk_reader_memory_manager.h"

#include <yt/client/chunk_client/data_statistics.h>

#include <yt/core/actions/future.h>

#include <yt/core/compression/public.h>

#include <yt/core/concurrency/async_semaphore.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/lazy_ptr.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref.h>

#include <yt/core/profiling/public.h>

namespace NYT::NChunkClient {

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
        int Index = -1;
        i64 UncompressedDataSize = 0;
        int Priority = 0;
    };

    TBlockFetcher(
        TBlockFetcherConfigPtr config,
        std::vector<TBlockInfo> blockInfos,
        TChunkReaderMemoryManagerPtr memoryManager,
        IChunkReaderPtr chunkReader,
        IBlockCachePtr blockCache,
        NCompression::ECodec codecId,
        double compressionRatio,
        const TClientBlockReadOptions& blockReadOptions);

    ~TBlockFetcher();

    //! Returns |true| if there are requested blocks that were not fetched enough times.
    bool HasMoreBlocks() const;

    //! Returns uncompressed size of block with given index.
    i64 GetBlockSize(int blockIndex) const;

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

    //! Returns codec and CPU time spent in compression.
    TCodecDuration GetDecompressionTime() const;

private:
    const TBlockFetcherConfigPtr Config_;
    std::vector<TBlockInfo> BlockInfos_;
    const IChunkReaderPtr ChunkReader_;
    const IBlockCachePtr BlockCache_;
    const IInvokerPtr CompressionInvoker_;
    const IInvokerPtr ReaderInvoker_;
    const double CompressionRatio_;
    const TChunkReaderMemoryManagerPtr MemoryManager_;
    NCompression::ICodec* const Codec_;
    const TClientBlockReadOptions BlockReadOptions_;
    NLogging::TLogger Logger;

    std::atomic<i64> UncompressedDataSize_ = {0};
    std::atomic<i64> CompressedDataSize_ = {0};
    std::atomic<NProfiling::TCpuDuration> DecompressionTime_ = {0};

    THashMap<int, int> BlockIndexToWindowIndex_;

    TFuture<TMemoryUsageGuardPtr> FetchNextGroupMemoryFuture_;

    struct TWindowSlot
    {
        // Created lazily in GetBlockPromise.
        TSpinLock BlockPromiseLock;
        TPromise<TBlock> BlockPromise;

        std::atomic<int> RemainingFetches = { 0 };

        TMemoryUsageGuardPtr MemoryUsageGuard;

        std::atomic_flag FetchStarted = ATOMIC_FLAG_INIT;
    };

    std::unique_ptr<TWindowSlot[]> Window_;
    int WindowSize_ = 0;

    int TotalRemainingFetches_ = 0;
    std::atomic<i64> TotalRemainingSize_ = { 0 };
    int FirstUnfetchedWindowIndex_ = 0;
    bool FetchingCompleted_ = false;

    void FetchNextGroup(TErrorOr<TMemoryUsageGuardPtr> memoryUsageGuardOrError);

    void RequestBlocks(
        const std::vector<int>& windowIndexes,
        const std::vector<int>& blockIndexes,
        i64 uncompressedSize);

    void OnGotBlocks(
        const std::vector<int>& windowIndexes,
        const std::vector<int>& blockIndexes,
        const TErrorOr<std::vector<TBlock>>& blocksOrError);

    void DecompressBlocks(
        const std::vector<int>& windowIndexes,
        const std::vector<TBlock>& compressedBlocks);

    void MarkFailedBlocks(
        const std::vector<int>& windowIndexes,
        const TError& error);

    void ReleaseBlock(int windowIndex);

    static TPromise<TBlock> GetBlockPromise(TWindowSlot& windowSlot);
    static void ResetBlockPromise(TWindowSlot& windowSlot);
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
        TChunkReaderMemoryManagerPtr memoryManager,
        IChunkReaderPtr chunkReader,
        IBlockCachePtr blockCache,
        NCompression::ECodec codecId,
        double compressionRatio,
        const TClientBlockReadOptions& blockReadOptions);

    TFuture<TBlock> FetchNextBlock();

    i64 GetNextBlockSize() const;

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

} // namespace NYT::NChunkClient
