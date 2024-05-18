#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf KafkaProxiesRootPath = "//sys/kafka_proxies";
constexpr TStringBuf KafkaProxiesInstancesPath = "//sys/kafka_proxies/instances";

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap;

DECLARE_REFCOUNTED_CLASS(TKafkaProxyConfig)
DECLARE_REFCOUNTED_CLASS(TKafkaProxyDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(IConnection)
DECLARE_REFCOUNTED_STRUCT(IServer)

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
