#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf KafkaProxiesRootPath = "//sys/kafka_proxies";
constexpr TStringBuf KafkaProxiesInstancesPath = "//sys/kafka_proxies/instances";

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TProxyBootstrapConfig)
DECLARE_REFCOUNTED_STRUCT(TProxyProgramConfig)
DECLARE_REFCOUNTED_STRUCT(TGroupCoordinatorConfig)
DECLARE_REFCOUNTED_STRUCT(TProxyDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(IBootstrap)
DECLARE_REFCOUNTED_STRUCT(IConnection)
DECLARE_REFCOUNTED_STRUCT(IServer)

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

DECLARE_REFCOUNTED_STRUCT(IGroupCoordinator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
