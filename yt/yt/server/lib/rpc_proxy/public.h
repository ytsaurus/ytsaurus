#pragma once

#include <yt/yt/client/api/rpc_proxy/public.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TApiServiceConfig)
DECLARE_REFCOUNTED_CLASS(TApiServiceDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TSecurityManagerConfig)

DECLARE_REFCOUNTED_STRUCT(IAccessChecker)
DECLARE_REFCOUNTED_STRUCT(IProxyCoordinator)
struct IBootstrap;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
