#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

struct IDistributedChunkWriter
    : virtual public TRefCounted
{
    virtual TFuture<void> WriteRecord(TSharedRef record) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedChunkWriter)

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkWriterPtr CreateDistributedChunkWriter(
    NNodeTrackerClient::TNodeDescriptor sequencerNode,
    NChunkClient::TSessionId sessionId,
    NApi::NNative::IConnectionPtr connection,
    TDistributedChunkWriterConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
