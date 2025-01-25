#pragma once

#include "chunk_reader.h"
#include "chunk_reader_options.h"
#include "chunk_reader_memory_manager.h"

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/concurrency/async_semaphore.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/lazy_ptr.h>
#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/profiling/public.h>

#include <library/cpp/yt/memory/ref.h>

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
        int ReaderIndex = -1;
        int BlockIndex = -1;
        int Priority = 0;

        i64 UncompressedDataSize = 0;

        //! This field is used when accessing block cache.
        //! May be |UncompressedData| or |None|.
        //! |None| is set to explicitly avoid accessing block cache.
        //! Block caches for other types of blocks are supported within other reading layers.
        EBlockType BlockType = EBlockType::None;
    };

    TBlockFetcher(
        TBlockFetcherConfigPtr config,
        std::vector<TBlockInfo> blockInfos,
        TChunkReaderMemoryManagerHolderPtr memoryManagerHolder,
        std::vector<IChunkReaderPtr> chunkReaders,
        IBlockCachePtr blockCache,
        NCompression::ECodec codecId,
        double compressionRatio,
        const TClientChunkReadOptions& chunkReadOptions,
        IInvokerPtr sessionInvoker = {});

    ~TBlockFetcher();

    //! Starts fetching blocks.
    void Start();

    //! Returns |true| if there are requested blocks that were not fetched enough times.
    bool HasMoreBlocks() const;

    //! Returns uncompressed size of block with given index.
    i64 GetBlockSize(int readerIndex, int blockIndex) const;
    i64 GetBlockSize(int blockIndex) const;

    //! Asynchronously fetches the block with given index.
    /*!
     *  It is not allowed to fetch the block more times than it has been requested.
     *  If an error occurs during fetching then the whole session is failed.
     */
    TFuture<TBlock> FetchBlock(int readerIndex, int blockIndex);
    TFuture<TBlock> FetchBlock(int blockIndex);

    //! Returns true if all blocks are fetched and false otherwise.
    bool IsFetchingCompleted() const;

    //! Returns total uncompressed size of read blocks.
    i64 GetUncompressedDataSize() const;

    //! Returns total compressed size of read blocks.
    i64 GetCompressedDataSize() const;

    //! Returns codec and CPU time spent in compression.
    TCodecDuration GetDecompressionTime() const;

    //! Signals that block fetcher finished reading from the reader with given index.
    //! It is guaranteed that the last call to OnReaderFinished happens-before the end
    //! of the last FetchBlock.
    DEFINE_SIGNAL(void(int), OnReaderFinished);

    //! Indicates that reads for the reader with given index be done under the given trace
    //! context. Must be called before fetching is started.
    void SetTraceContextForReader(int readerIndex, NTracing::TTraceContextPtr traceContext);

private:
    const TBlockFetcherConfigPtr Config_;
    std::vector<TBlockInfo> BlockInfos_;
    const IBlockCachePtr BlockCache_;
    const IInvokerPtr SessionInvoker_;
    const IInvokerPtr CompressionInvoker_;
    const IInvokerPtr ReaderInvoker_;
    const double CompressionRatio_;
    const TChunkReaderMemoryManagerHolderPtr MemoryManagerHolder_;
    NCompression::ICodec* const Codec_;
    const TClientChunkReadOptions ChunkReadOptions_;
    NLogging::TLogger Logger;

    struct TChunkState
    {
        IChunkReaderPtr Reader;
        THashMap<int, int> BlockIndexToWindowIndex;
        std::atomic<i64> RemainingBlockCount = 0;
        std::optional<NTracing::TTraceContextPtr> TraceContext;
    };

    std::vector<TChunkState> Chunks_;

    std::atomic<i64> UncompressedDataSize_ = 0;
    std::atomic<i64> CompressedDataSize_ = 0;
    std::atomic<NProfiling::TCpuDuration> DecompressionTime_ = 0;

    TFuture<TMemoryUsageGuardPtr> FetchNextGroupMemoryFuture_;

    struct TWindowSlot
    {
        // Created lazily in GetBlockPromise.
        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, BlockPromiseLock);
        TPromise<TBlock> BlockPromise;

        //! Number of not finished blocks. Used for firing OnReaderFinished signal.
        std::atomic<int> RemainingFetches = 0;

        TMemoryUsageGuardPtr MemoryUsageGuard;

        std::atomic_flag FetchStarted = ATOMIC_FLAG_INIT;
    };

    std::unique_ptr<TWindowSlot[]> Window_;

    int TotalRemainingFetches_ = 0;
    int FirstUnfetchedWindowIndex_ = 0;
    bool FetchingCompleted_ = false;

    //! Total size of not started blocks.
    std::atomic<i64> TotalRemainingSize_ = 0;

    std::atomic<bool> Started_ = false;

    struct TBlockDescriptor
    {
        int ReaderIndex;
        int BlockIndex;

        bool operator==(const TBlockDescriptor& other) const = default;
    };

    void FetchNextGroup(const TErrorOr<TMemoryUsageGuardPtr>& memoryUsageGuardOrError);

    void RequestBlocks(
        std::vector<int> windowIndexes,
        std::vector<TBlockDescriptor> blockDescriptor);

    void OnGotBlocks(
        int readerIndex,
        std::vector<int> windowIndexes,
        std::vector<int> blockIndices,
        TErrorOr<std::vector<TBlock>>&& blocksOrError);

    void DecompressBlocks(
        std::vector<int> windowIndexes,
        std::vector<TBlock> compressedBlocks);

    void MarkFailedBlocks(
        const std::vector<int>& windowIndexes,
        const TError& error);

    void ReleaseBlocks(const std::vector<int>& windowIndexes);

    static TPromise<TBlock> GetBlockPromise(TWindowSlot& windowSlot);
    static void ResetBlockPromise(TWindowSlot& windowSlot);

    void DoStartBlock(const TBlockInfo& blockInfo);
    void DoFinishBlock(const TBlockInfo& blockInfo);

    void DoSetBlock(const TBlockInfo& blockInfo, TWindowSlot& windowSlot, TBlock block);
    void DoSetError(const TBlockInfo& blockInfo, TWindowSlot& windowSlot, const TError& error);
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
        TChunkReaderMemoryManagerHolderPtr memoryManagerHolder,
        std::vector<IChunkReaderPtr> chunkReaders,
        IBlockCachePtr blockCache,
        NCompression::ECodec codecId,
        double compressionRatio,
        const TClientChunkReadOptions& chunkReadOptions,
        IInvokerPtr sessionInvoker = {});

    TFuture<TBlock> FetchNextBlock();

    i64 GetNextBlockSize() const;

    using TBlockFetcher::HasMoreBlocks;
    using TBlockFetcher::IsFetchingCompleted;
    using TBlockFetcher::GetUncompressedDataSize;
    using TBlockFetcher::GetCompressedDataSize;
    using TBlockFetcher::GetDecompressionTime;
    using TBlockFetcher::Start;

private:
    std::vector<TBlockInfo> OriginalOrderBlockInfos_;
    int CurrentIndex_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TSequentialBlockFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
