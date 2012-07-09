#pragma once

#include "public.h"
#include "private.h"
#include <ytlib/chunk_server/chunk_service.pb.h>

#include <ytlib/misc/metric.h>
#include <ytlib/misc/semaphore.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/async_stream_state.h>
#include <ytlib/actions/action_queue.h>
#include <ytlib/logging/tagged_logger.h>
#include <ytlib/chunk_holder/chunk_holder_service_proxy.h>
#include <ytlib/chunk_server/chunk_ypath_proxy.h>

#include <util/generic/deque.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

class TRemoteWriter
    : public TRefCounted
{
public:
    TRemoteWriter(
        const TRemoteWriterConfigPtr& config, 
        const TChunkId& chunkId,
        const std::vector<Stroka>& addresses);

    ~TRemoteWriter();

    void Open();

    bool IsReady() const;
    TAsyncError GetReadyEvent();

    void WriteBlock(const TSharedRef& block);
    void WriteBlock(std::vector<TSharedRef>&& vectorizedBlock);

    TAsyncError AsyncClose(const NChunkHolder::NProto::TChunkMeta& chunkMeta);

    const NChunkHolder::NProto::TChunkInfo& GetChunkInfo() const;
    const std::vector<Stroka> GetNodeAddresses() const;

    const TChunkId& GetChunkId() const;

    i64 GetCompressedSize() const;
    i64 GetUncompressedSize() const;
    double GetCompressionRatio() const;

    Stroka GetDebugInfo();

private:
    class TGroup;
    class TImpl;

    TIntrusivePtr<TImpl> Impl;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

