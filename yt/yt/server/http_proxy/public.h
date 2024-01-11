#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

DECLARE_REFCOUNTED_STRUCT(TLiveness)
DECLARE_REFCOUNTED_STRUCT(TProxyEntry)
DECLARE_REFCOUNTED_STRUCT(TCoordinatorProxy)

DECLARE_REFCOUNTED_STRUCT(IAccessChecker)

DECLARE_REFCOUNTED_CLASS(TProxyConfig)
DECLARE_REFCOUNTED_CLASS(TProxyDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TCoordinatorConfig)
DECLARE_REFCOUNTED_CLASS(TFramingConfig)
DECLARE_REFCOUNTED_CLASS(TTracingConfig)
DECLARE_REFCOUNTED_CLASS(TApiConfig)
DECLARE_REFCOUNTED_CLASS(TApiDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TAccessCheckerConfig)
DECLARE_REFCOUNTED_CLASS(TAccessCheckerDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(IDynamicConfigManager)

DECLARE_REFCOUNTED_CLASS(TApi)
DECLARE_REFCOUNTED_CLASS(TCoordinator)
DECLARE_REFCOUNTED_CLASS(THostsHandler)
DECLARE_REFCOUNTED_CLASS(TClusterConnectionHandler)
DECLARE_REFCOUNTED_CLASS(THttpProxyHeapUsageProfiler)
DECLARE_REFCOUNTED_CLASS(TPingHandler)
DECLARE_REFCOUNTED_CLASS(TDiscoverVersionsHandler)
DECLARE_REFCOUNTED_CLASS(THttpAuthenticator)
DECLARE_REFCOUNTED_CLASS(TCompositeHttpAuthenticator)

DECLARE_REFCOUNTED_CLASS(TContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
