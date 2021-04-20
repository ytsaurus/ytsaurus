#include "config.h"
#include "private.h"

#include <yt/yt/server/http_proxy/clickhouse/config.h>

#include <yt/yt/ytlib/auth/config.h>

#include <yt/yt/ytlib/security_client/config.h>

#include <yt/yt/client/driver/config.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/core/https/config.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NHttpProxy {

using namespace NYTree;
using namespace NAuth;

////////////////////////////////////////////////////////////////////////////////

TCoordinatorConfig::TCoordinatorConfig()
{
    RegisterParameter("enable", Enable)
        .Default(false);
    RegisterParameter("announce", Announce)
        .Default(true);

    RegisterParameter("public_fqdn", PublicFqdn)
        .Default();
    RegisterParameter("default_role_filter", DefaultRoleFilter)
        .Default("data");

    RegisterParameter("heartbeat_interval", HeartbeatInterval)
        .Default(TDuration::Seconds(5));
    RegisterParameter("death_age", DeathAge)
        .Default(TDuration::Minutes(2));

    RegisterParameter("show_ports", ShowPorts)
        .Default(false);

    RegisterParameter("load_average_weight", LoadAverageWeight)
        .Default(1.0);
    RegisterParameter("network_load_weight", NetworkLoadWeight)
        .Default(50);
    RegisterParameter("randomness_weight", RandomnessWeight)
        .Default(1);
    RegisterParameter("dampening_weight", DampeningWeight)
        .Default(0.3);
}

////////////////////////////////////////////////////////////////////////////////

TApiTestingOptions::TDelayBeforeCommand::TDelayBeforeCommand()
{
    RegisterParameter("delay", Delay);
    RegisterParameter("parameter_path", ParameterPath);
    RegisterParameter("substring", Substring);
}

TApiTestingOptions::TApiTestingOptions()
{
    RegisterParameter("delay_before_command", DelayBeforeCommand)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TFramingConfig::TFramingConfig()
{
    RegisterParameter("keep_alive_period", KeepAlivePeriod)
        .Default(TDuration::Seconds(5));

    RegisterParameter("enable", Enable)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

TApiConfig::TApiConfig()
{
    RegisterParameter("ban_cache_expiration_time", BanCacheExpirationTime)
        .Default(TDuration::Seconds(60));

    RegisterParameter("concurrency_limit", ConcurrencyLimit)
        .Default(1024);

    RegisterParameter("disable_cors_check", DisableCorsCheck)
        .Default(false);

    RegisterParameter("force_tracing", ForceTracing)
        .Default(false);

    RegisterParameter("testing", TestingOptions)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TApiDynamicConfig::TApiDynamicConfig()
{
    RegisterParameter("framing", Framing)
        .DefaultNew();

    RegisterParameter("formats", Formats)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TAccessCheckerConfig::TAccessCheckerConfig()
{
    RegisterParameter("enabled", Enabled)
        .Default(false);

    RegisterParameter("path_prefix", PathPrefix)
        .Default("//sys/http_proxy_roles");

    RegisterParameter("cache", Cache)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TAccessCheckerDynamicConfig::TAccessCheckerDynamicConfig()
{
    RegisterParameter("enabled", Enabled)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TProxyConfig::TProxyConfig()
{
    RegisterParameter("port", Port)
        .Default(80);
    RegisterParameter("thread_count", ThreadCount)
        .Default(16);
    RegisterParameter("http_server", HttpServer)
        .DefaultNew();
    RegisterParameter("https_server", HttpsServer)
        .Optional();

    RegisterPostprocessor([&] {
        HttpServer->Port = Port;
    });

    RegisterParameter("driver", Driver)
        .Default();
    RegisterParameter("auth", Auth)
        .DefaultNew();

    RegisterParameter("ui_redirect_url", UIRedirectUrl)
        .Default();

    RegisterParameter("retry_request_queue_size_limit_exceeded", RetryRequestQueueSizeLimitExceeded)
        .Default(true);

    RegisterParameter("coordinator", Coordinator)
        .DefaultNew();
    RegisterParameter("api", Api)
        .DefaultNew();

    RegisterParameter("access_checker", AccessChecker)
        .DefaultNew();

    RegisterParameter("clickhouse", ClickHouse)
        .DefaultNew();

    RegisterParameter("cypress_annotations", CypressAnnotations)
        .Default(NYTree::BuildYsonNodeFluently()
            .BeginMap()
            .EndMap()
        ->AsMap());

    RegisterParameter("abort_on_unrecognized_options", AbortOnUnrecognizedOptions)
        .Default(false);

    RegisterParameter("default_network", DefaultNetwork)
        .Default(NBus::DefaultNetworkName);
    RegisterParameter("networks", Networks)
        .Default();

    RegisterParameter("dynamic_config_manager", DynamicConfigManager)
        .DefaultNew();

    RegisterParameter("dynamic_config_path", DynamicConfigPath)
        .Default("//sys/proxies/@config");
    RegisterParameter("use_tagged_dynamic_config", UseTaggedDynamicConfig)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TProxyDynamicConfig::TProxyDynamicConfig()
{
    RegisterParameter("api", Api)
        .Default();

    RegisterParameter("tracing", Tracing)
        .DefaultNew();

    RegisterParameter("fitness_function", FitnessFunction)
        .Default();

    RegisterParameter("relax_csrf_check", RelaxCsrfCheck)
        .Default(false);

    RegisterParameter("cpu_weight", CpuWeight)
        .Default(1);
    RegisterParameter("cpu_wait_weight", CpuWaitWeight)
        .Default(10);
    RegisterParameter("concurrent_requests_weight", ConcurrentRequestsWeight)
        .Default(10);

    RegisterParameter("clickhouse", ClickHouse)
        .DefaultNew();

    RegisterParameter("formats", Formats)
        .Default();

    RegisterParameter("framing", Framing)
        .DefaultNew();

    RegisterParameter("access_checker", AccessChecker)
        .DefaultNew();

    // COMPAT(gritukan, levysotsky)
    RegisterPostprocessor([&] {
        if (!Api) {
            Api = New<TApiDynamicConfig>();
            Api->Formats = Formats;
            Api->Framing = Framing;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
