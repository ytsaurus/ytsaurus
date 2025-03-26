#pragma once

#include <yt/yt/server/lib/rpc_proxy/public.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TProxyBootstrapConfig)
DECLARE_REFCOUNTED_STRUCT(TProxyProgramConfig)
DECLARE_REFCOUNTED_STRUCT(TProxyDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TProxyMemoryLimits)
DECLARE_REFCOUNTED_STRUCT(TAccessCheckerConfig)
DECLARE_REFCOUNTED_STRUCT(TAccessCheckerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TDiscoveryServiceConfig)
DECLARE_REFCOUNTED_STRUCT(TBundleProxyDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TBootstrap)
DECLARE_REFCOUNTED_CLASS(IBundleDynamicConfigManager)
DECLARE_REFCOUNTED_STRUCT(IDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
