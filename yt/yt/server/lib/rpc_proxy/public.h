#pragma once

#include <yt/yt/client/api/rpc_proxy/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TApiServiceConfig)
DECLARE_REFCOUNTED_STRUCT(TApiServiceDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TUserAccessValidatorDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TMultiproxyPresetDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TMultiproxyDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TStructuredLoggingTopicDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TStructuredLoggingMethodDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TQueryCorpusReporterConfig)

DECLARE_REFCOUNTED_CLASS(TProxyHeapUsageProfiler)
DECLARE_REFCOUNTED_STRUCT(TApiTestingOptions)
DECLARE_REFCOUNTED_CLASS(TMulticonnectionClientCache)

DECLARE_REFCOUNTED_STRUCT(IAccessChecker)
DECLARE_REFCOUNTED_STRUCT(IProxyCoordinator)
DECLARE_REFCOUNTED_STRUCT(IApiService)
DECLARE_REFCOUNTED_STRUCT(IQueryCorpusReporter)
DECLARE_REFCOUNTED_STRUCT(IMultiproxyAccessValidator)

enum class EMultiproxyEnabledMethods;
enum class EMultiproxyMethodKind;

// Pair <cluster-name,user-identity>
using TMultiConnectionClientCacheKey = std::pair<std::optional<std::string>, NRpc::TAuthenticationIdentity>;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NProfiling::TProfiler, RpcProxyProfiler, "/rpc_proxy");
YT_DEFINE_GLOBAL(const NLogging::TLogger, RpcProxyLogger, "RpcProxy");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
