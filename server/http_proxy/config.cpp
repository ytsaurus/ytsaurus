#include "config.h"
#include "private.h"

#include <yt/server/http_proxy/clickhouse/config.h>

#include <yt/ytlib/auth/config.h>

#include <yt/client/driver/config.h>

#include <yt/client/api/config.h>

#include <yt/core/https/config.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NHttpProxy {

using namespace NYTree;
using namespace NAuth;

////////////////////////////////////////////////////////////////////////////////

INodePtr ConvertAuthFromLegacyConfig(const INodePtr& legacyConfig)
{
    if (!legacyConfig->AsMap()->FindChild("authentication")) {
        return BuildYsonNodeFluently().BeginMap().EndMap();
    }

    auto legacyAuthentication = legacyConfig->AsMap()->GetChild("authentication")->AsMap();
    auto grant = legacyAuthentication->FindChild("grant");
    if (!grant) {
        grant = ConvertToNode("");
    }

    auto config = BuildYsonNodeFluently().BeginMap()
        .Item("auth").BeginMap()
            .Item("enable_authentication").Value(legacyAuthentication->GetChild("enable"))
            .Item("blackbox_service").BeginMap().EndMap()
            .Item("cypress_token_authenticator").BeginMap().EndMap()
            .Item("blackbox_token_authenticator").BeginMap()
                .Item("scope").Value(grant)
            .EndMap()
            .Item("blackbox_cookie_authenticator").BeginMap().EndMap()
        .EndMap().EndMap();

    auto csrfSecret = legacyAuthentication->FindChild("csrf_secret");
    if (csrfSecret) {
        config->AsMap()->AddChild("csrf_secret", CloneNode(csrfSecret));
    }

    auto csrfTokenTtl = legacyAuthentication->FindChild("csrf_token_ttl");
    if (csrfTokenTtl) {
        config->AsMap()->AddChild("csrf_token_ttl", CloneNode(csrfTokenTtl));
    }
    return config;
}

INodePtr ConvertHttpsFromLegacyConfig(const INodePtr& legacyConfig)
{
    auto sslPort = legacyConfig->AsMap()->FindChild("ssl_port");
    if (!sslPort) {
        return BuildYsonNodeFluently().BeginMap().EndMap();
    }

    return BuildYsonNodeFluently().BeginMap()
        .Item("https_server").BeginMap()
            .Item("port").Value(sslPort)
            .Item("credentials")
                .BeginMap()
                    .Item("private_key").BeginMap()
                        .Item("file_name").Value(legacyConfig->AsMap()->GetChild("ssl_key"))
                    .EndMap()
                    .Item("cert_chain").BeginMap()
                        .Item("file_name").Value(legacyConfig->AsMap()->GetChild("ssl_certificate"))
                    .EndMap()
                .EndMap()
            .EndMap()
        .EndMap();
}

INodePtr ConvertFromLegacyConfig(const INodePtr& legacyConfig)
{
    auto redirect = legacyConfig->AsMap()->FindChild("redirect");
    if (redirect) {
        redirect = redirect->AsList()->GetChild(0)->AsList()->GetChild(1);
    }

    auto proxy = legacyConfig->AsMap()->GetChild("proxy")->AsMap();

    auto config =  BuildYsonNodeFluently()
        .BeginMap()
            .Item("port").Value(legacyConfig->AsMap()->GetChild("port"))
            .Item("coordinator").Value(legacyConfig->AsMap()->GetChild("coordination"))
            .Item("logging").Value(proxy->GetChild("logging"))
            .Item("driver").Value(proxy->GetChild("driver"))
            .Item("api").BeginMap().EndMap()
            .OptionalItem("ui_redirect_url", redirect)
        .EndMap();

    if (auto monitoringPort = legacyConfig->AsMap()->FindChild("monitoring_port")) {
        config->AsMap()->AddChild("monitoring_port", CloneNode(monitoringPort));
    }

    if (auto node = legacyConfig->AsMap()->FindChild("cypress_annotations")) {
        config->AsMap()->AddChild("cypress_annotations", CloneNode(node));
    }

    if (auto node = legacyConfig->AsMap()->FindChild("disable_cors_check")) {
        config->AsMap()->GetChild("api")->AsMap()->AddChild("disable_cors_check", CloneNode(node));
    }

    if (auto node = legacyConfig->AsMap()->FindChild("api")) {
        if (auto forceTracing = node->AsMap()->FindChild("force_tracing")) {
            config->AsMap()->GetChild("api")->AsMap()->AddChild(
                "force_tracing",
                CloneNode(forceTracing));
        }
    }

    if (auto node = proxy->FindChild("address_resolver")) {
        config->AsMap()->AddChild("address_resolver", CloneNode(node));
    }

    if (auto node = proxy->FindChild("chunk_client_dispatcher")) {
        config->AsMap()->AddChild("chunk_client_dispatcher", CloneNode(node));
    }

    if (auto node = legacyConfig->AsMap()->FindChild("show_ports")) {
        config->AsMap()->GetChild("coordinator")->AsMap()->AddChild("show_ports", CloneNode(node));
    }

    if (auto node = proxy->FindChild("clickhouse")) {
        config->AsMap()->AddChild("clickhouse", CloneNode(node));
    }

    config = PatchNode(config, ConvertAuthFromLegacyConfig(legacyConfig));
    config = PatchNode(config, ConvertHttpsFromLegacyConfig(legacyConfig));

    return config;
}

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

TApiTestingOptions::TDelayInsideGet::TDelayInsideGet()
{
    RegisterParameter("delay", Delay);
    RegisterParameter("path", Path);
}

TApiTestingOptions::TApiTestingOptions()
{
    RegisterParameter("delay_inside_get", DelayInsideGet)
        .Optional();
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

    RegisterParameter("framing_keep_alive_period", FramingKeepAlivePeriod)
        .Default();

    RegisterParameter("testing", TestingOptions)
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

    RegisterParameter("clickhouse", ClickHouse)
        .DefaultNew();

    RegisterParameter("cypress_annotations", CypressAnnotations)
        .Default(NYTree::BuildYsonNodeFluently()
            .BeginMap()
            .EndMap()
        ->AsMap());

    RegisterParameter("abort_on_unrecognized_options", AbortOnUnrecognizedOptions)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TDynamicConfig::TDynamicConfig()
{
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

    RegisterParameter("datalens_tracing_override", DatalensTracingOverride)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
