#include "config.h"

#include "private.h"

#include <yt/yt/server/http_proxy/clickhouse/config.h>

#include <yt/yt/server/lib/zookeeper_proxy/config.h>

#include <yt/yt/server/lib/cypress_registrar/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/security_client/config.h>

#include <yt/yt/library/auth_server/config.h>

#include <yt/yt/client/driver/config.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/core/https/config.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NHttpProxy {

using namespace NYTree;
using namespace NAuth;

////////////////////////////////////////////////////////////////////////////////

void TCoordinatorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);
    registrar.Parameter("announce", &TThis::Announce)
        .Default(true);

    registrar.Parameter("public_fqdn", &TThis::PublicFqdn)
        .Default();
    registrar.Parameter("default_role_filter", &TThis::DefaultRoleFilter)
        .Default(NApi::DefaultHttpProxyRole);

    registrar.Parameter("heartbeat_interval", &TThis::HeartbeatInterval)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("death_age", &TThis::DeathAge)
        .Default(TDuration::Minutes(2));
    registrar.Parameter("cypress_timeout", &TThis::CypressTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("show_ports", &TThis::ShowPorts)
        .Default(false);

    registrar.Parameter("load_average_weight", &TThis::LoadAverageWeight)
        .Default(1.0);
    registrar.Parameter("network_load_weight", &TThis::NetworkLoadWeight)
        .Default(50);
    registrar.Parameter("randomness_weight", &TThis::RandomnessWeight)
        .Default(1);
    registrar.Parameter("dampening_weight", &TThis::DampeningWeight)
        .Default(0.3);

    registrar.Parameter("cypress_registrar", &TThis::CypressRegistrar)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        config->CypressRegistrar->RequestTimeout = config->CypressTimeout;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDelayBeforeCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("delay", &TThis::Delay);
    registrar.Parameter("parameter_path", &TThis::ParameterPath);
    registrar.Parameter("substring", &TThis::Substring);
}

void TApiTestingOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("delay_before_command", &TThis::DelayBeforeCommand)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TFramingConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("keep_alive_period", &TThis::KeepAlivePeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TApiConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("ban_cache_expiration_time", &TThis::BanCacheExpirationTime)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("concurrency_limit", &TThis::ConcurrencyLimit)
        .Default(1024);

    registrar.Parameter("cors", &TThis::Cors)
        .DefaultNew();

    registrar.Parameter("force_tracing", &TThis::ForceTracing)
        .Default(false);

    registrar.Parameter("testing", &TThis::TestingOptions)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TApiDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("framing", &TThis::Framing)
        .DefaultNew();

    registrar.Parameter("formats", &TThis::Formats)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TAccessCheckerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);

    registrar.Parameter("path_prefix", &TThis::PathPrefix)
        .Default("//sys/http_proxy_roles");

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
    registrar.Parameter("port", &TThis::Port)
        .Default(80);
    registrar.Parameter("thread_count", &TThis::ThreadCount)
        .Default(16);
    registrar.Parameter("http_server", &TThis::HttpServer)
        .DefaultNew();
    registrar.Parameter("https_server", &TThis::HttpsServer)
        .Optional();
    registrar.Parameter("tvm_only_http_server", &TThis::TvmOnlyHttpServer)
        .Optional();
    registrar.Parameter("tvm_only_https_server", &TThis::TvmOnlyHttpsServer)
        .Optional();

    registrar.Postprocessor([] (TThis* config) {
        config->HttpServer->Port = config->Port;
    });

    registrar.Parameter("driver", &TThis::Driver)
        .Default();
    registrar.Parameter("auth", &TThis::Auth)
        .DefaultNew();
    registrar.Parameter("tvm_only_auth", &TThis::TvmOnlyAuth)
        .Optional();

    registrar.Parameter("ui_redirect_url", &TThis::UIRedirectUrl)
        .Default();

    registrar.Parameter("retry_request_queue_size_limit_exceeded", &TThis::RetryRequestQueueSizeLimitExceeded)
        .Default(true);

    registrar.Parameter("role", &TThis::Role)
        .Default(NApi::DefaultHttpProxyRole);

    registrar.Parameter("coordinator", &TThis::Coordinator)
        .DefaultNew();
    registrar.Parameter("api", &TThis::Api)
        .DefaultNew();

    registrar.Parameter("access_checker", &TThis::AccessChecker)
        .DefaultNew();

    registrar.Parameter("clickhouse", &TThis::ClickHouse)
        .DefaultNew();

    registrar.Parameter("cypress_annotations", &TThis::CypressAnnotations)
        .Default(NYTree::BuildYsonNodeFluently()
            .BeginMap()
            .EndMap()
        ->AsMap());

    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);

    registrar.Parameter("default_network", &TThis::DefaultNetwork)
        .Default(NBus::DefaultNetworkName);
    registrar.Parameter("networks", &TThis::Networks)
        .Default();

    registrar.Parameter("dynamic_config_manager", &TThis::DynamicConfigManager)
        .DefaultNew();

    registrar.Parameter("dynamic_config_path", &TThis::DynamicConfigPath)
        .Default("//sys/http_proxies/@config");
    registrar.Parameter("use_tagged_dynamic_config", &TThis::UseTaggedDynamicConfig)
        .Default(false);

    registrar.Parameter("zookeeper_proxy", &TThis::ZookeeperProxy)
        .Default();

    registrar.Preprocessor([] (TThis* config) {
        config->ClusterConnectionDynamicConfigPolicy = NApi::NNative::EClusterConnectionDynamicConfigPolicy::FromClusterDirectoryWithStaticPatch;
    });

    registrar.Postprocessor([] (TThis* config) {
        if (!config->TvmOnlyAuth && config->Auth && config->Auth->TvmService) {
            auto auth = New<TAuthenticationManagerConfig>();
            auth->TvmService = CloneYsonStruct(config->Auth->TvmService);
            auth->BlackboxService = CloneYsonStruct(config->Auth->BlackboxService);
            auth->BlackboxTicketAuthenticator = CloneYsonStruct(config->Auth->BlackboxTicketAuthenticator);

            config->TvmOnlyAuth = auth;
        }
    });

    registrar.Postprocessor([] (TThis* config) {
        if ((config->TvmOnlyHttpServer || config->TvmOnlyHttpsServer) && !config->TvmOnlyAuth) {
            THROW_ERROR_EXCEPTION("\"tvm_only_auth\" must be configured when using \"tvm_only_http_server\" or \"tvm_only_https_server\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TProxyDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("api", &TThis::Api)
        .Default();

    registrar.Parameter("tracing", &TThis::Tracing)
        .DefaultNew();

    registrar.Parameter("fitness_function", &TThis::FitnessFunction)
        .Default();

    registrar.Parameter("relax_csrf_check", &TThis::RelaxCsrfCheck)
        .Default(false);

    registrar.Parameter("cpu_weight", &TThis::CpuWeight)
        .Default(1);
    registrar.Parameter("cpu_wait_weight", &TThis::CpuWaitWeight)
        .Default(10);
    registrar.Parameter("concurrent_requests_weight", &TThis::ConcurrentRequestsWeight)
        .Default(10);

    registrar.Parameter("clickhouse", &TThis::ClickHouse)
        .DefaultNew();

    registrar.Parameter("formats", &TThis::Formats)
        .Default();

    registrar.Parameter("framing", &TThis::Framing)
        .DefaultNew();

    registrar.Parameter("access_checker", &TThis::AccessChecker)
        .DefaultNew();

    registrar.Parameter("cluster_connection", &TThis::ClusterConnection)
        .DefaultNew();

    // COMPAT(gritukan, levysotsky)
    registrar.Postprocessor([] (TThis* config) {
        if (!config->Api) {
            config->Api = New<TApiDynamicConfig>();
            config->Api->Formats = config->Formats;
            config->Api->Framing = config->Framing;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
