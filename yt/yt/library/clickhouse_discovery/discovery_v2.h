#pragma once

#include "config.h"

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

IDiscoveryPtr CreateDiscoveryV2(
    TDiscoveryV2ConfigPtr config,
    NApi::NNative::IConnectionPtr connection,
    NRpc::IChannelFactoryPtr channelFactory,
    IInvokerPtr invoker,
    std::vector<TString> extraAttributes,
    NLogging::TLogger logger = {},
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
