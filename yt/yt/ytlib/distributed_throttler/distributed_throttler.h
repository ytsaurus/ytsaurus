#pragma once

#include "public.h"

#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/rpc/channel.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/ytlib/discovery_client/public.h>

namespace NYT::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

struct IDistributedThrottlerFactory
    : public virtual TRefCounted
{
    virtual NConcurrency::IReconfigurableThroughputThrottlerPtr GetOrCreateThrottler(
        const TString& throttlerId,
        NConcurrency::TThroughputThrottlerConfigPtr throttlerConfig,
        TDuration throttleRpcTimeout = DefaultThrottleRpcTimeout) = 0;

    virtual void Reconfigure(TDistributedThrottlerConfigPtr config) = 0;

    virtual void Start() = 0;
    virtual void Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedThrottlerFactory)

IDistributedThrottlerFactoryPtr CreateDistributedThrottlerFactory(
    TDistributedThrottlerConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory,
    IInvokerPtr invoker,
    NDiscoveryClient::TGroupId groupId,
    NDiscoveryClient::TMemberId memberId,
    NRpc::IServerPtr rpcServer,
    TString address,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedThrottler
