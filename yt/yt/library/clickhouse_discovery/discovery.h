#pragma once

#include "public.h"

#include <yt/yt/library/discovery_client/config.h>
#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

IDiscoveryPtr CreateDiscoveryFromNativeConnection(
    TDiscoveryConfigPtr config,
    NApi::NNative::IConnectionPtr connection,
    NRpc::IChannelFactoryPtr channelFactory,
    IInvokerPtr invoker,
    std::vector<std::string> extraAttributes,
    NLogging::TLogger logger = {},
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
