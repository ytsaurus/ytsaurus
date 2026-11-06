#include "yt_runtime.h"

#include <yt/yt/core/service_discovery/yp/config.h>
#include <yt/yt/core/service_discovery/yp/service_discovery.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/dispatcher.h>
#include <yt/yt/core/rpc/config.h>
#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/library/profiling/solomon/registry.h>

namespace NYql {

void ConfigureYtRuntime(bool enableYpDiscovery)
{
    // This executable has no YT metrics collector; pending registrations would retain sensors.
    NYT::NProfiling::TSolomonRegistry::Get()->Disable();

    // Configure YT RPC client to use single threads for dispatching and transport
    // (as long as each query have its own RPC client instance)

    auto busDispatcherConfig = NYT::New<NYT::NBus::NTcp::TDispatcherConfig>();
    busDispatcherConfig->ThreadPoolSize = 1;
    NYT::NBus::NTcp::TDispatcher::Get()->Configure(busDispatcherConfig);

    auto rpcDispatcherConfig = NYT::New<NYT::NRpc::TDispatcherConfig>();
    rpcDispatcherConfig->HeavyPoolSize = 1;
    rpcDispatcherConfig->CompressionPoolSize = 1;
    NYT::NRpc::TDispatcher::Get()->Configure(rpcDispatcherConfig);

    auto* dispatcher = NYT::NRpc::TDispatcher::Get();
    if (enableYpDiscovery && !dispatcher->GetServiceDiscovery()) {
        dispatcher->SetServiceDiscovery(NYT::NServiceDiscovery::NYP::CreateServiceDiscovery(
            NYT::New<NYT::NServiceDiscovery::NYP::TServiceDiscoveryConfig>()));
    }
}

} // namespace NYql
