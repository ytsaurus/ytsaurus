#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf KafkaProxiesRootPath = "//sys/kafka_proxies";
constexpr TStringBuf KafkaProxiesInstancesPath = "//sys/kafka_proxies/instances";

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TProxyBootstrapConfig)
DECLARE_REFCOUNTED_CLASS(TProxyProgramConfig)
DECLARE_REFCOUNTED_CLASS(TProxyDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(IBootstrap)
DECLARE_REFCOUNTED_STRUCT(IConnection)
DECLARE_REFCOUNTED_STRUCT(IServer)

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
