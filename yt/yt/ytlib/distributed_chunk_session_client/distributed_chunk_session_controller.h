#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

struct IDistributedChunkSessionController
    : virtual public TRefCounted
{
    // Starts session and returns the address of distributed write coordinator.
    virtual TFuture<NNodeTrackerClient::TNodeDescriptor> StartSession() = 0;

    virtual bool IsActive() = 0;

    // Must be the last call.
    // Finishes and confirms chunk.
    virtual TFuture<void> Close() = 0;

    virtual NChunkClient::TSessionId GetSessionId() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedChunkSessionController)

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionControllerPtr CreateDistributedChunkSessionController(
    NApi::NNative::IClientPtr client,
    TDistributedChunkSessionControllerConfigPtr config,
    NObjectClient::TTransactionId transactionId,
    std::vector<std::string> columns,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
