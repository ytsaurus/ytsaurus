#pragma once

#include "public.h"
#include "private.h"
#include "async_writer.h"
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
    : public IAsyncWriter
{
public:
    TRemoteWriter(
        const TRemoteWriterConfigPtr& config, 
        const TChunkId& chunkId,
        const std::vector<Stroka>& addresses);

    ~TRemoteWriter();

    virtual void Open();

    virtual bool TryWriteBlock(const TSharedRef& block);
    virtual TAsyncError GetReadyEvent();

    virtual TAsyncError AsyncClose(const NChunkHolder::NProto::TChunkMeta& chunkMeta);

    virtual const NChunkHolder::NProto::TChunkInfo& GetChunkInfo() const;
    const std::vector<Stroka> GetNodeAddresses() const;
    const TChunkId& GetChunkId() const;

    Stroka GetDebugInfo();

private:
    class TImpl;
    //! A group is a bunch of blocks that is sent in a single RPC request.
    class TGroup;
    typedef TIntrusivePtr<TGroup> TGroupPtr;

    struct TNode;
    typedef TIntrusivePtr<TNode> TNodePtr;

    typedef ydeque<TGroupPtr> TWindow;

    typedef NChunkHolder::TChunkHolderServiceProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

    TIntrusivePtr<TImpl> Impl;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

