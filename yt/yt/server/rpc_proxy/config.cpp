#include "config.h"

#include <yt/yt/server/lib/cypress_registrar/config.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

void TDiscoveryServiceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);
    registrar.Parameter("liveness_update_period", &TThis::LivenessUpdatePeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("proxy_update_period", &TThis::ProxyUpdatePeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("availability_period", &TThis::AvailabilityPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("backoff_period", &TThis::BackoffPeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("cypress_registrar", &TThis::CypressRegistrar)
        .DefaultNew();

    registrar.Postprocessor([] (TThis* config) {
        if (config->AvailabilityPeriod <= config->LivenessUpdatePeriod) {
            THROW_ERROR_EXCEPTION("Availability period must be greater than liveness update period")
                << TErrorAttribute("availability_period", config->AvailabilityPeriod)
                << TErrorAttribute("liveness_update_period", config->LivenessUpdatePeriod);
        }
        if (config->BackoffPeriod <= config->AvailabilityPeriod) {
            THROW_ERROR_EXCEPTION("Backoff period must be greater than availability period")
                << TErrorAttribute("availability_period", config->AvailabilityPeriod)
                << TErrorAttribute("backoff_period", config->BackoffPeriod);
        }

        if (!config->CypressRegistrar->AliveChildTtl) {
            config->CypressRegistrar->AliveChildTtl = config->AvailabilityPeriod;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TAccessCheckerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);

    registrar.Parameter("path_prefix", &TThis::PathPrefix)
        .Default("//sys/rpc_proxy_roles");

    registrar.Parameter("use_access_control_objects", &TThis::UseAccessControlObjects)
        .Default(false);

    registrar.Parameter("cache", &TThis::Cache)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TAccessCheckerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TProxyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("grpc_server", &TThis::GrpcServer)
        .Default();
    registrar.Parameter("tvm_only_auth", &TThis::TvmOnlyAuth)
        .Optional();
    registrar.Parameter("api_service", &TThis::ApiService)
        .DefaultNew();
    registrar.Parameter("discovery_service", &TThis::DiscoveryService)
        .DefaultNew();
    registrar.Parameter("addresses", &TThis::Addresses)
        .Default();
    registrar.Parameter("worker_thread_pool_size", &TThis::WorkerThreadPoolSize)
        .GreaterThan(0)
        .Default(8);

    registrar.Parameter("access_checker", &TThis::AccessChecker)
        .DefaultNew();

    registrar.Parameter("cypress_annotations", &TThis::CypressAnnotations)
        .Default(NYTree::BuildYsonNodeFluently()
            .BeginMap()
            .EndMap()
        ->AsMap());

    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);

    registrar.Parameter("retry_request_queue_size_limit_exceeded", &TThis::RetryRequestQueueSizeLimitExceeded)
        .Default(true);

    registrar.Parameter("dynamic_config_manager", &TThis::DynamicConfigManager)
        .DefaultNew();

    registrar.Parameter("dynamic_config_path", &TThis::DynamicConfigPath)
        .Default("//sys/rpc_proxies/@config");
    registrar.Parameter("use_tagged_dynamic_config", &TThis::UseTaggedDynamicConfig)
        .Default(false);

    registrar.Parameter("role", &TThis::Role)
        .Default(NApi::DefaultRpcProxyRole);

    registrar.Parameter("memory_limits", &TThis::MemoryLimits)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        config->DynamicConfigManager->IgnoreConfigAbsence = true;
    });

    registrar.Postprocessor([] (TThis* config) {
        if (config->GrpcServer && config->GrpcServer->Addresses.size() > 1) {
            THROW_ERROR_EXCEPTION("Multiple GRPC addresses are not supported");
        }
    });

    registrar.Postprocessor([] (TThis* config) {
        if (!config->TvmOnlyAuth && config->TvmService) {
            auto auth = New<TAuthenticationManagerConfig>();
            auth->TvmService = CloneYsonStruct(config->TvmService);
            auth->BlackboxService = CloneYsonStruct(config->BlackboxService);
            auth->BlackboxTicketAuthenticator = CloneYsonStruct(config->BlackboxTicketAuthenticator);

            config->TvmOnlyAuth = auth;
        }
    });

    registrar.Preprocessor([] (TThis* config) {
        config->ClusterConnectionDynamicConfigPolicy = NApi::NNative::EClusterConnectionDynamicConfigPolicy::FromClusterDirectoryWithStaticPatch;
    });

    registrar.Preprocessor([] (TThis* config) {
        // Setting sane total memory limit for rpc proxy.
        config->MemoryLimits->Total = 20_GB;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TProxyDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("api", &TThis::Api)
        .DefaultNew();

    registrar.Parameter("tracing", &TThis::Tracing)
        .DefaultNew();
    registrar.Parameter("formats", &TThis::Formats)
        .Default();

    registrar.Parameter("access_checker", &TThis::AccessChecker)
        .DefaultNew();

    registrar.Parameter("rpc_server", &TThis::RpcServer)
        .DefaultNew();

    registrar.Parameter("cluster_connection", &TThis::ClusterConnection)
        .DefaultNew();

    registrar.Parameter("memory_limits", &TThis::MemoryLimits)
        .DefaultNew();

    // COMPAT(gritukan, levysotsky)
    registrar.Postprocessor([] (TThis* config) {
        if (config->Api->Formats.empty()) {
            config->Api->Formats = config->Formats;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TProxyMemoryLimits::Register(TRegistrar registrar)
{
    registrar.Parameter("total", &TThis::Total)
        .Optional();
    registrar.Parameter("rpc", &TThis::Rpc)
        .Optional();
    registrar.Parameter("lookup", &TThis::Lookup)
        .Optional();
    registrar.Parameter("query", &TThis::Query)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
