#pragma once

#include "public.h"

#include <core/actions/callback.h>
#include <core/concurrency/action_queue.h>

#include <core/misc/ref.h>
#include <core/misc/async_stream_state.h>

#include <core/concurrency/async_semaphore.h>

#include <core/compression/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

class TEncodingWriter
    : public TRefCounted
{
    DECLARE_BYVAL_RO_PROPERTY(i64, UncompressedSize);
    DECLARE_BYVAL_RO_PROPERTY(i64, CompressedSize);
    DECLARE_BYVAL_RO_PROPERTY(double, CompressionRatio);

public:
    TEncodingWriter(
        TEncodingWriterConfigPtr config,
        TEncodingWriterOptionsPtr options,
        IChunkWriterPtr chunkWriter);

    bool IsReady() const;
    TAsyncError GetReadyEvent();

    void WriteBlock(const TSharedRef& block);
    void WriteBlock(std::vector<TSharedRef>&& vectorizedBlock);

    // Future is set when all block get written to underlying writer.
    TAsyncError Flush();

    ~TEncodingWriter();

private:
    TAtomic UncompressedSize_;
    TAtomic CompressedSize_;

    // Protects #CompressionRatio_.
    TSpinLock SpinLock;
    double CompressionRatio_;

    TEncodingWriterConfigPtr Config;
    IChunkWriterPtr ChunkWriter;

    IInvokerPtr CompressionInvoker;
    NConcurrency::TAsyncSemaphore Semaphore;
    NCompression::ICodec* Codec;

    TAsyncStreamState State;

    std::deque<TSharedRef> PendingBlocks;

    // True if OnReadyEventCallback is subscribed on AsyncWriter::ReadyEvent.
    bool IsWaiting;
    bool CloseRequested;
    TCallback<void(TError)> OnReadyEventCallback;
    TCallback<void()> TriggerWritingCallback;

    void OnReadyEvent(TError error);
    void TriggerWriting();
    void WritePendingBlocks();

    void ProcessCompressedBlock(const TSharedRef& block, i64 delta);

    void DoCompressBlock(const TSharedRef& block);
    void DoCompressVector(const std::vector<TSharedRef>& vectorizedBlock);

    void VerifyBlock(
        const TSharedRef& origin,
        const TSharedRef& compressedBlock);

    void VerifyVector(
        const std::vector<TSharedRef>& origin,
        const TSharedRef& compressedBlock);

    void SetCompressionRatio(double value);

};

DEFINE_REFCOUNTED_TYPE(TEncodingWriter)

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
