#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>

namespace NYT::NDistributedChunkSession {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Thread affinity: any
 */
struct IDistributedChunkSessionManager
    : virtual public TRefCounted
{
    virtual IDistributedChunkSessionCoordinatorPtr FindCoordinator(NChunkClient::TSessionId sessionId) const = 0;

    virtual IDistributedChunkSessionCoordinatorPtr GetCoordinatorOrThrow(NChunkClient::TSessionId sessionId) const = 0;

    virtual IDistributedChunkSessionCoordinatorPtr StartSession(
        NChunkClient::TSessionId sessionId,
        std::vector<NNodeTrackerClient::TNodeDescriptor> targets) = 0;

    virtual void RenewLease(NChunkClient::TSessionId sessionId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedChunkSessionManager)

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionManagerPtr CreateDistributedChunkSessionManager(
    TDistributedChunkSessionServiceConfigPtr config,
    IInvokerPtr invoker,
    NApi::NNative::IConnectionPtr connection);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSession
