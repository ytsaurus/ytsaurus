#pragma once

#include <yt/core/misc/intrusive_ptr.h>

#include <yt/client/api/rpc_proxy/public.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

DECLARE_REFCOUNTED_CLASS(TCellProxyConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TApiServiceConfig)
DECLARE_REFCOUNTED_CLASS(TDiscoveryServiceConfig)

DECLARE_REFCOUNTED_STRUCT(IProxyCoordinator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
