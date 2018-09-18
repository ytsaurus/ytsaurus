#pragma once

#include <yt/core/misc/intrusive_ptr.h>
#include <yt/core/misc/enum.h>

#include <yt/client/api/rpc_proxy/public.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

DECLARE_REFCOUNTED_CLASS(TCellProxyConfig)

DECLARE_REFCOUNTED_STRUCT(IProxyCoordinator)

DECLARE_REFCOUNTED_CLASS(TApiServiceConfig)
DECLARE_REFCOUNTED_CLASS(TDiscoveryServiceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
