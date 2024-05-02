#pragma once

#include "public.h"
#include "component_discovery.h"

#include <yt/yt/library/profiling/solomon/proxy.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

//! Works for all components represented in the `EYTComponentType` enum.
class TYTComponentEndpointProvider
    : public NProfiling::IEndpointProvider
    , public TComponentDiscoverer
{
public:
    TYTComponentEndpointProvider(
        NApi::IClientPtr client,
        TYTComponentEndpointProviderConfigPtr config);

    TString GetComponentName() const override;
    // TODO(achulkov2): Introduce client-side caching, either in the endpoint provider hierarchy, or in the component discoverer itself.
    std::vector<TEndpoint> GetEndpoints() const override;

private:
    const TYTComponentEndpointProviderConfigPtr Config_;
};

////////////////////////////////////////////////////////////////////////////////

//! Creates a proxying handler that allows collecting metrics from internal YT components via http proxies.
NProfiling::TSolomonProxyPtr CreateSolomonProxy(
    const TSolomonProxyConfigPtr& config,
    const NApi::IClientPtr& client,
    NConcurrency::IPollerPtr poller);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
