#pragma once

#include "public.h"
#include "block.h"
#include "data_statistics.h"

#include <yt/core/compression/public.h>

#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/nonblocking_queue.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NChunkClient {

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
        const NLogging::TLogger& logger);

    bool IsReady() const;
    TFuture<void> GetReadyEvent();

    void WriteBlock(TSharedRef block);
    void WriteBlock(std::vector<TSharedRef> vectorizedBlock);

    // Future is set when all block get written to underlying writer.
    TFuture<void> Flush();

    const TCodecDuration& GetCompressionDuration() const;

private:
    const TEncodingWriterConfigPtr Config_;
    const TEncodingWriterOptionsPtr Options_;
    const IChunkWriterPtr ChunkWriter_;
    const IBlockCachePtr BlockCache_;

    const NLogging::TLogger Logger;
    const NConcurrency::TAsyncSemaphorePtr Semaphore_;
    NCompression::ICodec* const Codec_;
    const IInvokerPtr CompressionInvoker_;
    const TCallback<void(const TErrorOr<TBlock>&)> WritePendingBlockCallback_;

    std::atomic<double> CompressionRatio_ = {0};
    std::atomic<i64> UncompressedSize_ = {0};
    std::atomic<i64> CompressedSize_ = {0};

    NChunkClient::TCodecDuration CodecTime_;

    int AddedBlockIndex_ = 0;
    int WrittenBlockIndex_ = 0;

    NConcurrency::TNonblockingQueue<TBlock> PendingBlocks_;

    TFuture<void> OpenFuture_;
    TPromise<void> CompletionError_ = NewPromise<void>();

private:
    void WritePendingBlock(const TErrorOr<TBlock>& blockOrError);

    void EnsureOpen();
    void CacheUncompressedBlock(const TSharedRef& block, int blockIndex);

    void DoCompressBlock(const TSharedRef& uncompressedBlock);
    void DoCompressVector(const std::vector<TSharedRef>& uncompressedVectorizedBlock);

    void ProcessCompressedBlock(const TBlock& block, i64 delta);

    void VerifyBlock(
        const TSharedRef& uncompressedBlock,
        const TSharedRef& compressedBlock);

    void VerifyVector(
        const std::vector<TSharedRef>& uncompressedVectorizedBlock,
        const TSharedRef& compressedBlock);
};

DEFINE_REFCOUNTED_TYPE(TEncodingWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
