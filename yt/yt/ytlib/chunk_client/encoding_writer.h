#pragma once

#include "block.h"

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/nonblocking_queue.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TEncodingWriter
    : public TRefCounted
{
public:
    DECLARE_BYVAL_RO_PROPERTY(i64, UncompressedSize);
    DECLARE_BYVAL_RO_PROPERTY(i64, CompressedSize);
    DECLARE_BYVAL_RO_PROPERTY(double, CompressionRatio);

public:
    TEncodingWriter(
        TEncodingWriterConfigPtr config,
        TEncodingWriterOptionsPtr options,
        IChunkWriterPtr chunkWriter,
        IBlockCachePtr blockCache,
        NLogging::TLogger logger);

    bool IsReady() const;
    TFuture<void> GetReadyEvent();

    void WriteBlock(
        TSharedRef block,
        EBlockType blockType,
        std::optional<int> groupIndex = std::nullopt);
    void WriteBlock(
        std::vector<TSharedRef> vectorizedBlock,
        EBlockType blockType,
        std::optional<int> groupIndex = std::nullopt);

    // Future is set when all block get written to underlying writer.
    TFuture<void> Flush();

    TCodecDuration GetCompressionDuration() const;

private:
    const TEncodingWriterConfigPtr Config_;
    const TEncodingWriterOptionsPtr Options_;
    const IChunkWriterPtr ChunkWriter_;
    const IBlockCachePtr BlockCache_;

    const NLogging::TLogger Logger;
    const NConcurrency::TAsyncSemaphorePtr SizeSemaphore_;
    const NConcurrency::TAsyncSemaphorePtr CodecSemaphore_;
    NCompression::ICodec* const Codec_;
    const IInvokerPtr CompressionInvoker_;
    const TCallback<void(const TErrorOr<TBlock>&)> WritePendingBlockCallback_;

    std::atomic<double> CompressionRatio_ = {0};
    std::atomic<i64> UncompressedSize_ = {0};
    std::atomic<i64> CompressedSize_ = {0};

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, CodecTimeLock_);
    NChunkClient::TCodecDuration CodecTime_;

    int AddedBlockIndex_ = 0;
    int WrittenBlockIndex_ = 0;

    NConcurrency::TNonblockingQueue<TBlock> PendingBlocks_;

    TFuture<void> OpenFuture_;
    TPromise<void> CompletionError_ = NewPromise<void>();

private:
    void WritePendingBlock(const TErrorOr<TBlock>& blockOrError);

    void EnsureOpen();
    void CacheUncompressedBlock(const TSharedRef& block, EBlockType blockType, int blockIndex);

    TBlock DoCompressBlock(
        const TSharedRef& uncompressedBlock,
        EBlockType blockType,
        int blockIndex,
        std::optional<int> groupIndex,
        NConcurrency::TAsyncSemaphoreGuard&& guard);
    TBlock DoCompressVector(
        const std::vector<TSharedRef>& uncompressedVectorizedBlock,
        EBlockType blockType,
        int blockIndex,
        std::optional<int> groupIndex,
        NConcurrency::TAsyncSemaphoreGuard&& guard);

    void ProcessCompressedBlock(i64 delta);

    void VerifyBlock(
        const TSharedRef& uncompressedBlock,
        const TSharedRef& compressedBlock);

    void VerifyVector(
        const std::vector<TSharedRef>& uncompressedVectorizedBlock,
        const TSharedRef& compressedBlock);
};

DEFINE_REFCOUNTED_TYPE(TEncodingWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
