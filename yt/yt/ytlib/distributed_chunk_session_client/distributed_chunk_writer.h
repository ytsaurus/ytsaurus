#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

struct IDistributedChunkWriter
    : virtual public TRefCounted
{
    //! Writes bunch of rows. Returns an asynchronous flag that is set when
    //! the rows are successfully written.
    virtual TFuture<void> Write(TSharedRange<NTableClient::TUnversionedRow> rows) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedChunkWriter)

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkWriterPtr CreateDistributedChunkWriter(
    NNodeTrackerClient::TNodeDescriptor coordinatorNode,
    NChunkClient::TSessionId sessionId,
    NApi::NNative::IConnectionPtr connection,
    TDistributedChunkWriterConfigPtr config,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
