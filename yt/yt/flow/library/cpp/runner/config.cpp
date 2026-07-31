#include "config.h"

#include <yt/yt/flow/library/cpp/common/authenticator.h>
#include <yt/yt/flow/library/cpp/companion/config.h>
#include <yt/yt/flow/library/cpp/controller/config.h>
#include <yt/yt/flow/library/cpp/worker/config.h>

#include <yt/yt/library/profiling/solomon/proxy.h>
#include <yt/yt/library/tcmalloc/config.h>
#include <yt/yt/library/tracing/jaeger/config.h>

#include <yt/yt/core/net/local_address.h>
#include <yt/yt/core/ypath/helpers.h>

namespace NYT::NFlow {

using namespace NAuth;
using namespace NTracing;

using NYT::NYPath::YPathJoin;

////////////////////////////////////////////////////////////////////////////////

void TFlowNodeConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster_url", &TThis::ClusterUrl)
        .NonEmpty();
    registrar.Parameter("path", &TThis::Path)
        .NonEmpty();
    registrar.Parameter("proxy_role", &TThis::ProxyRole)
        .Default();

    registrar.Parameter("clients_cache", &TThis::ClientsCache)
        .DefaultNew();

    registrar.Parameter("rpc_port", &TThis::RpcPort)
        .Default(0)
        .GreaterThanOrEqual(0)
        .LessThan(65536);
    registrar.Parameter("monitoring_port", &TThis::MonitoringPort)
        .Default(0)
        .GreaterThanOrEqual(0)
        .LessThan(65536);

    registrar.Parameter("companion", &TThis::Companion)
        .Default();

    registrar.Parameter("controller", &TThis::Controller)
        .DefaultNew();

    registrar.Parameter("worker", &TThis::Worker)
        .DefaultNew();

    registrar.Parameter("authenticator", &TThis::Authenticator)
        .DefaultNew();
    registrar.Parameter("tvm", &TThis::Tvm)
        .DefaultNew();
    registrar.Parameter("bus_server", &TThis::BusServer)
        .DefaultNew();
    registrar.Parameter("rpc_server", &TThis::RpcServer)
        .DefaultNew();
    registrar.Parameter("core_dumper", &TThis::CoreDumper)
        .Default();
    registrar.Parameter("solomon_exporter", &TThis::SolomonExporter)
        .DefaultNew();
    registrar.Parameter("solomon_proxy", &TThis::SolomonProxy)
        .DefaultNew();

    registrar.Parameter("http_client_config", &TThis::HttpClientConfig)
        .DefaultNew();
    registrar.Parameter("https_client_config", &TThis::HttpsClientConfig)
        .DefaultNew();
    registrar.Parameter("http_poller_threads", &TThis::HttpPollerThreads)
        .GreaterThan(0)
        .Default(1);

    // TODO(mikari):
    // Change default to false.
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(true);

    registrar.Parameter("ignore_singletons_dynamic_config", &TThis::IgnoreSingletonsDynamicConfig)
        .Default(false);

    registrar.Parameter("enable_phdr_cache", &TThis::EnablePhdrCache)
        .Default(true);

    registrar.Preprocessor([] (TThis* config) {
        config->Tvm->ClientSelfIdEnv = "TVM_ID";
        config->Tvm->ClientSelfSecretEnv = "TVM_SECRET";
        config->Tvm->ClientEnableServiceTicketFetching = true;
        config->Tvm->ClientEnableServiceTicketChecking = true;
        config->Tvm->ClientDstMap = GetDefaultTvmAliases();
        config->Tvm->EnableTicketParseCache = true;

        // Emit per-sensor yt_aggr=<rule> aggregation labels instead of the legacy yt_aggr=1.
        config->SolomonExporter->EnableSolomonAggregates = true;

        // Guard roughly one small allocation per 16 MiB allocated with GWP-ASAN to catch heap corruption.
        config->GetSingletonConfig<NTCMalloc::TTCMallocConfig>()->GuardedSamplingRate = 16_MB;
    });

    registrar.Postprocessor([] (TThis* config) {
        if (config->RpcPort > 0) {
            if ((config->BusServer->Port && config->BusServer->Port != config->RpcPort) || config->BusServer->UnixDomainSocketPath) {
                THROW_ERROR_EXCEPTION("Explicit socket configuration for bus server is forbidden");
            }
            config->BusServer->Port = config->RpcPort;
        }

        // Set defaults in case that ClientDstMap is overwritten.
        for (const auto& [alias, tvmId] : GetDefaultTvmAliases()) {
            config->Tvm->ClientDstMap.try_emplace(alias, tvmId);
        }

        THROW_ERROR_EXCEPTION_IF(config->TryGetSingletonConfig<TJaegerTracerConfig>()->TvmService, "TVM for jaeger tracer must be configured via \"tvm\" parameter");
    });
}

NHttp::TServerConfigPtr TFlowNodeConfig::CreateMonitoringHttpServerConfig()
{
    auto config = New<NHttp::TServerConfig>();
    config->Port = MonitoringPort;
    config->BindRetryCount = BusServer->BindRetryCount;
    config->BindRetryBackoff = BusServer->BindRetryBackoff;
    config->ServerName = "HttpMon";
    return config;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
