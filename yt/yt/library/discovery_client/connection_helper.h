#pragma once

#include "public.h"

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/yt/core/rpc/public.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

//! Fetches discovery_connection from //sys/@cluster_connection via the public HTTP client.
TDiscoveryConnectionConfigPtr FetchDiscoveryConnectionConfig(IClientPtr client);

NRpc::IChannelFactoryPtr CreateDefaultDiscoveryChannelFactory();

IDiscoveryClientPtr CreateDiscoveryClientFromYtProxy(
    IClientPtr client,
    TDiscoveryClientConfigPtr clientConfig,
    NRpc::IChannelFactoryPtr channelFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
