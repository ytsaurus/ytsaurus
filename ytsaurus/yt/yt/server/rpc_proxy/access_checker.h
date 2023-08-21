#pragma once

#include "public.h"

#include <yt/yt/server/lib/rpc_proxy/public.h>

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

IAccessCheckerPtr CreateAccessChecker(
    TAccessCheckerConfigPtr config,
    IProxyCoordinatorPtr proxyCoordinator,
    NApi::NNative::IConnectionPtr connection,
    IDynamicConfigManagerPtr dynamicConfigManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
