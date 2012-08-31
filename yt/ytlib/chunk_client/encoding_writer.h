#pragma once

#include "public.h"

#include <ytlib/actions/callback.h>
#include <ytlib/actions/action_queue.h>

#include <ytlib/misc/ref.h>
#include <ytlib/misc/semaphore.h>
#include <ytlib/misc/async_stream_state.h>

#include <ytlib/codecs/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

class TEncodingWriter
    : public TRefCounted
{
    DEFINE_BYVAL_RO_PROPERTY(i64, UncompressedSize);
    DEFINE_BYVAL_RO_PROPERTY(i64, CompressedSize);
    DEFINE_BYVAL_RO_PROPERTY(double, CompressionRatio);

public:
    TEncodingWriter(TEncodingWriterConfigPtr config, IAsyncWriterPtr asyncWriter);

    bool IsReady() const;
    TAsyncError GetReadyEvent();

    void WriteBlock(const TSharedRef& block);
    void WriteBlock(std::vector<TSharedRef>&& vectorizedBlock);

    // Future is set when all block get written to underlying writer.
    TAsyncError AsyncFlush();

    ~TEncodingWriter();

private:
    TEncodingWriterConfigPtr Config;
    IAsyncWriterPtr AsyncWriter;

    IInvokerPtr CompressionInvoker;
    TAsyncSemaphore Semaphore;
    TCodecPtr Codec;

    TAsyncStreamState State;

    std::deque<TSharedRef> PendingBlocks;

    TCallback<void(TError)> WritePending;


    void WritePendingBlocks(TError error);
    void ProcessCompressedBlock(const TSharedRef& block, int delta);

    void DoCompressBlock(const TSharedRef& block);
    void DoCompressVector(const std::vector<TSharedRef>& vectorizedBlock);

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
