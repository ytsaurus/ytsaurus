#pragma once

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/actions/public.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(IDistributedSessionCoordinator)
DECLARE_REFCOUNTED_CLASS(TNodePinger)
DECLARE_REFCOUNTED_STRUCT(TSessionCommon)

using TSessionId = TGuid;

////////////////////////////////////////////////////////////////////////////////

struct TSessionOptions
{
    TDuration ControlRpcTimeout;
    TDuration PingPeriod;
    TDuration RetentionTime;
};

////////////////////////////////////////////////////////////////////////////////

class IDistributedSessionCoordinator
    : public TRefCounted
{
public:
    virtual TSessionId GetId() const = 0;

    virtual void BindToRemoteSessionIfNecessary(TString address) = 0;

    virtual void PingAbortCallback(TError error) = 0;

    virtual void CloseRemoteInstances() = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedSessionCoordinator)

////////////////////////////////////////////////////////////////////////////////

IDistributedSessionCoordinatorPtr CreateDistributeSessionCoordinator(
    TSessionOptions options,
    IInvokerPtr invoker,
    NNodeTrackerClient::INodeChannelFactoryPtr channelFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
