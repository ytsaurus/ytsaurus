#pragma once

#include "public.h"
#include "private.h"
#include "async_writer.h"
#include "chunk_ypath_proxy.h"
#include "data_node_service_proxy.h"
#include "node_directory.h"

#include <util/generic/deque.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

class TRemoteWriter
    : public IAsyncWriter
{
public:
    TRemoteWriter(
        TRemoteWriterConfigPtr config,
        const TChunkId& chunkId,
        const std::vector<TNodeDescriptor>& targets);

    ~TRemoteWriter();

    virtual void Open();

    virtual bool TryWriteBlock(const TSharedRef& block);
    virtual TAsyncError GetReadyEvent();

    virtual TAsyncError AsyncClose(const NChunkClient::NProto::TChunkMeta& chunkMeta);

    virtual const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const;
    std::vector<int> GetWrittenIndexes() const;
    const TChunkId& GetChunkId() const;

    Stroka GetDebugInfo();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

