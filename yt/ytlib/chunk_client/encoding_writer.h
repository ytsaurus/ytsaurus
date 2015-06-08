#pragma once

#include "public.h"

#include <core/actions/callback.h>

#include <core/misc/ref.h>
#include <core/misc/async_stream_state.h>

#include <core/concurrency/async_semaphore.h>

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

    ~TEncodingWriter();

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

    std::atomic<i64> UncompressedSize_ = {0};
    std::atomic<i64> CompressedSize_ = {0};

    int AddedBlockIndex_ = 0;
    int WrittenBlockIndex_ = 0;

    std::atomic<double> CompressionRatio_;

    IInvokerPtr CompressionInvoker_;
    NConcurrency::TAsyncSemaphore Semaphore_;
    NCompression::ICodec* Codec_;

    TAsyncStreamState State_;

    std::deque<TSharedRef> PendingBlocks_;

    // True if OnReadyEventCallback_ is subscribed on AsyncWriter::ReadyEvent.
    bool IsWaiting_ = false;
    bool CloseRequested_ = false;
    TCallback<void(const TError&)> OnReadyEventCallback_;
    TCallback<void()> TriggerWritingCallback_;

    NLogging::TLogger Logger;


    void OnReadyEvent(const TError& error);
    void TriggerWriting();
    void WritePendingBlocks();

    void DoCompressBlock(const TSharedRef& uncompressedBlock);
    void DoCompressVector(const std::vector<TSharedRef>& uncompressedVectorizedBlock);

    void ProcessCompressedBlock(const TSharedRef& block, i64 delta);

    void VerifyBlock(
        const TSharedRef& uncompressedBlock,
        const TSharedRef& compressedBlock);

    void VerifyVector(
        const std::vector<TSharedRef>& uncompressedVectorizedBlock,
        const TSharedRef& compressedBlock);

    void SetCompressionRatio(double value);

};

DEFINE_REFCOUNTED_TYPE(TEncodingWriter)

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
