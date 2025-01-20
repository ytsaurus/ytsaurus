#pragma once

#include "public.h"
#include "component_discovery.h"

#include <yt/yt/library/profiling/solomon/proxy.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

//! Creates a proxying handler that allows collecting metrics from internal YT components via http proxies.
NProfiling::TSolomonProxyPtr CreateSolomonProxy(
    const TSolomonProxyConfigPtr& config,
    const TComponentDiscoveryOptions& componentDiscoveryOptions,
    const NApi::NNative::IClientPtr& client,
    NConcurrency::IPollerPtr poller);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
