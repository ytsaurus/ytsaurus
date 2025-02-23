#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NTcpProxy {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf TcpProxiesRootPath = "//sys/tcp_proxies";
constexpr TStringBuf TcpProxiesInstancesPath = "//sys/tcp_proxies/instances";
constexpr TStringBuf TcpProxiesRoutesPath = "//sys/tcp_proxies/routes";

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IBootstrap)

DECLARE_REFCOUNTED_STRUCT(TProxyBootstrapConfig)
DECLARE_REFCOUNTED_STRUCT(TProxyProgramConfig)
DECLARE_REFCOUNTED_STRUCT(TProxyDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TRouterConfig)
DECLARE_REFCOUNTED_STRUCT(TRouterDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(IRouter)

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTcpProxy
