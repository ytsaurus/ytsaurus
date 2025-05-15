#pragma once

#include <yt/yt/client/api/rpc_proxy/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TApiServiceConfig)
DECLARE_REFCOUNTED_CLASS(TApiServiceDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TSecurityManagerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TUserAccessValidatorDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TMultiproxyPresetDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TMultiproxyDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TStructuredLoggingTopicDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TStructuredLoggingMethodDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TQueryCorpusReporterConfig)

DECLARE_REFCOUNTED_CLASS(TProxyHeapUsageProfiler)
DECLARE_REFCOUNTED_CLASS(TApiTestingOptions)

DECLARE_REFCOUNTED_STRUCT(IAccessChecker)
DECLARE_REFCOUNTED_STRUCT(IProxyCoordinator)
DECLARE_REFCOUNTED_STRUCT(ISecurityManager)
DECLARE_REFCOUNTED_STRUCT(IApiService)
DECLARE_REFCOUNTED_STRUCT(IQueryCorpusReporter)
DECLARE_REFCOUNTED_STRUCT(IMultiproxyAccessValidator)

enum class EMultiproxyEnabledMethods;
enum class EMultiproxyMethodKind;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NProfiling::TProfiler, RpcProxyProfiler, "/rpc_proxy");
YT_DEFINE_GLOBAL(const NLogging::TLogger, RpcProxyLogger, "RpcProxy");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
