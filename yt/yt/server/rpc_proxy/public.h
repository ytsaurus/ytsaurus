#pragma once

#include <yt/yt/core/misc/intrusive_ptr.h>

#include <yt/yt/client/api/rpc_proxy/public.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

DECLARE_REFCOUNTED_CLASS(TProxyConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicProxyConfig)
DECLARE_REFCOUNTED_CLASS(TSecurityManagerConfig)
DECLARE_REFCOUNTED_CLASS(TApiServiceConfig)
DECLARE_REFCOUNTED_CLASS(TAccessCheckerConfig)
DECLARE_REFCOUNTED_CLASS(TAccessCheckerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TDiscoveryServiceConfig)

DECLARE_REFCOUNTED_STRUCT(IProxyCoordinator)
DECLARE_REFCOUNTED_STRUCT(IAccessChecker)

DECLARE_REFCOUNTED_CLASS(TSecurityManger)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
