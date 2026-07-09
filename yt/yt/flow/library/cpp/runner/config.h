#pragma once

#include "public.h"

#include <yt/yt/client/cache/config.h>

#include <yt/yt/library/coredumper/config.h>
#include <yt/yt/library/cypress_election/public.h>
#include <yt/yt/library/profiling/solomon/config.h>
#include <yt/yt/library/program/config.h>
#include <yt/yt/library/tracing/jaeger/public.h>
#include <yt/yt/library/tvm/service/config.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/http/config.h>
#include <yt/yt/core/https/config.h>
#include <yt/yt/core/rpc/config.h>
#include <yt/yt/core/ypath/public.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TFlowNodeConfig
    : public virtual TSingletonsConfig
{
    std::string ClusterUrl;
    NYPath::TYPath Path;
    std::optional<std::string> ProxyRole;

    NClient::NCache::TClientsCacheConfigPtr ClientsCache;

    int RpcPort{};
    int MonitoringPort{};

    NCompanion::TCompanionConfigPtr Companion;

    NController::TControllerConfigPtr Controller;
    NWorker::TWorkerConfigPtr Worker;

    TAuthenticatorConfigPtr Authenticator;

    NAuth::TTvmServiceConfigPtr Tvm;
    NBus::NTcp::TBusServerConfigPtr BusServer;
    NRpc::TServerConfigPtr RpcServer;
    NCoreDump::TCoreDumperConfigPtr CoreDumper;
    NProfiling::TSolomonExporterConfigPtr SolomonExporter;
    NProfiling::TSolomonProxyConfigPtr SolomonProxy;
    NHttp::TClientConfigPtr HttpClientConfig;
    NHttps::TClientConfigPtr HttpsClientConfig;
    int HttpPollerThreads{};

    bool AbortOnUnrecognizedOptions{};

    // Can be enabled to disable singletons dynamic config in an emergency
    // (controller is crashing with current singletons dynamic config).
    bool IgnoreSingletonsDynamicConfig{};

    // Should be disabled whenever computation uses shared objects
    // as it breaks exception handling.
    bool EnablePhdrCache{};

    NYT::NHttp::TServerConfigPtr CreateMonitoringHttpServerConfig();

    REGISTER_YSON_STRUCT(TFlowNodeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFlowNodeConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
