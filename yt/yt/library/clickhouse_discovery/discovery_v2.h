#pragma once

#include "config.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

IDiscoveryPtr CreateDiscoveryV2(
    TDiscoveryV2ConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory,
    IInvokerPtr invoker,
    std::vector<TString> extraAttributes,
    NLogging::TLogger logger = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
