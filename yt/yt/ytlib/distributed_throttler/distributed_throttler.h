#pragma once

#include "public.h"

#include <yt/core/actions/invoker.h>

#include <yt/core/rpc/channel.h>

#include <yt/core/concurrency/public.h>

#include <yt/ytlib/discovery_client/public.h>

namespace NYT::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

class TDistributedThrottlerFactory
    : public TRefCounted
{
public:
    TDistributedThrottlerFactory(
        TDistributedThrottlerConfigPtr config,
        NRpc::IChannelFactoryPtr channelFactory,
        IInvokerPtr invoker,
        NDiscoveryClient::TGroupId groupId,
        NDiscoveryClient::TMemberId memberId,
        NRpc::IServerPtr rpcServer,
        TString address,
        NLogging::TLogger logger);
    ~TDistributedThrottlerFactory();

    NConcurrency::IReconfigurableThroughputThrottlerPtr GetOrCreateThrottler(
        const TString& throttlerId,
        NConcurrency::TThroughputThrottlerConfigPtr throttlerConfig);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TDistributedThrottlerFactory)

////////////////////////////////////////////////////////////////////////////////

NConcurrency::IReconfigurableThroughputThrottlerPtr CreateDistributedThrottler(
    TDistributedThrottlerConfigPtr config,
    NConcurrency::TThroughputThrottlerConfigPtr throttlerConfig,
    NRpc::IChannelFactoryPtr channelFactory,
    IInvokerPtr invoker,
    NDiscoveryClient::TGroupId groupId,
    NDiscoveryClient::TMemberId memberId,
    NRpc::IServerPtr rpcServer,
    TString address,
    NLogging::TLogger logger = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
