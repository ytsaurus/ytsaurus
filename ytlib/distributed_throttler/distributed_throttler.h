#pragma once

#include "public.h"

#include <yt/core/actions/invoker.h>

#include <yt/core/rpc/channel.h>

#include <yt/core/concurrency/public.h>

#include <yt/ytlib/discovery_client/public.h>

namespace NYT::NDistributedThrottler {

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
