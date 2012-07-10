#pragma once

#include "public.h"

#include <ytlib/actions/callback.h>
#include <ytlib/misc/semaphore.h>
#include <ytlib/misc/async_stream_state.h>
#include <ytlib/misc/codec.h>

#include <util/thread/lfqueue.h>

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

private:
    TEncodingWriterConfigPtr Config;
    IAsyncWriterPtr AsyncWriter;

    TAsyncSemaphore Semaphore;
    ICodec* Codec;

    TAsyncStreamState State;

    TLockFreeQueue<TClosure> CompressionTasks;
    std::deque<TSharedRef> PendingBlocks;

    TClosure CompressNext;
    TCallback<void(TError)> WritePending;


    void WritePendingBlocks(TError error);
    void ProcessCompressedBlock(const TSharedRef& block, int delta);

    void Compress();
    void DoCompressBlock(const TSharedRef& block);
    void DoCompressVector(const std::vector<TSharedRef>& vectorizedBlock);

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
