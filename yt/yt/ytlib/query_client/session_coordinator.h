#pragma once

#include "public.h"

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/actions/public.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TDistributedSessionOptions
{
    TDuration ControlRpcTimeout;
    TDuration PingPeriod;
    TDuration RetentionTime;
};

////////////////////////////////////////////////////////////////////////////////

struct IDistributedSessionCoordinator
    : public TRefCounted
{
    virtual TDistributedSessionId GetDistributedSessionId() const = 0;

    //! Forces coordinator to start pinging a remote session created by another remote session.
    virtual void BindToRemoteSession(TString address) = 0;

    //! Cancels query execution initiated by coordinator.
    virtual void Abort(TError error) = 0;

    //! Closes remote sessions and terminates pingers.
    virtual void CloseRemoteSessions() = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedSessionCoordinator)

////////////////////////////////////////////////////////////////////////////////

IDistributedSessionCoordinatorPtr CreateDistributeSessionCoordinator(
    NNodeTrackerClient::INodeChannelFactoryPtr channelFactory,
    IInvokerPtr invoker,
    TDistributedSessionOptions options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
