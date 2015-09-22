#pragma once

#include "public.h"

#include <core/misc/ref.h>

#include <core/concurrency/async_semaphore.h>
#include <core/concurrency/nonblocking_queue.h>

#include <core/compression/public.h>

#include <core/logging/log.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

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
        IBlockCachePtr blockCache);

    bool IsReady() const;
    TFuture<void> GetReadyEvent();

    void WriteBlock(TSharedRef block);
    void WriteBlock(std::vector<TSharedRef> vectorizedBlock);

    // Future is set when all block get written to underlying writer.
    TFuture<void> Flush();

private:
    const TEncodingWriterConfigPtr Config_;
    TEncodingWriterOptionsPtr Options_;
    const IChunkWriterPtr ChunkWriter_;
    const IBlockCachePtr BlockCache_;

    std::atomic<i64> UncompressedSize_ = { 0 };
    std::atomic<i64> CompressedSize_ = { 0 };

    int AddedBlockIndex_ = 0;
    int WrittenBlockIndex_ = 0;

    std::atomic<double> CompressionRatio_;

    IInvokerPtr CompressionInvoker_;
    NConcurrency::TAsyncSemaphore Semaphore_;
    NCompression::ICodec* Codec_;

    NConcurrency::TNonblockingQueue<TSharedRef> PendingBlocks_;

    TPromise<void> CompletionError_ = NewPromise<void>();
    TCallback<void(const TErrorOr<TSharedRef>& blockOrError)> WritePendingBlockCallback_;

    bool CloseRequested_ = false;

    NLogging::TLogger Logger;

    void WritePendingBlock(const TErrorOr<TSharedRef>& blockOrError);

    void DoCompressBlock(const TSharedRef& uncompressedBlock);
    void DoCompressVector(const std::vector<TSharedRef>& uncompressedVectorizedBlock);

    void ProcessCompressedBlock(const TSharedRef& block, i64 delta);

    void VerifyBlock(
        const TSharedRef& uncompressedBlock,
        const TSharedRef& compressedBlock);

    void VerifyVector(
        const std::vector<TSharedRef>& uncompressedVectorizedBlock,
        const TSharedRef& compressedBlock);

};

DEFINE_REFCOUNTED_TYPE(TEncodingWriter)

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
