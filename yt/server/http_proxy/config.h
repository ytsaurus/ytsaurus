#pragma once

#include "public.h"

#include <yt/server/lib/misc/config.h>

#include <yt/ytlib/auth/config.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/client/driver/config.h>

#include <yt/client/api/config.h>

#include <yt/core/https/config.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

class TCoordinatorConfig
    : public NYTree::TYsonSerializable
{
public:
    bool Enable;
    bool Announce;

    std::optional<TString> PublicFqdn;

    TDuration HeartbeatInterval;
    TDuration DeathAge;

    bool ShowPorts;

    double LoadAverageWeight;
    double NetworkLoadWeight;
    double RandomnessWeight;
    double DampeningWeight;

    TCoordinatorConfig()
    {
        RegisterParameter("enable", Enable)
            .Default(false);
        RegisterParameter("announce", Announce)
            .Default(true);

        RegisterParameter("public_fqdn", PublicFqdn)
            .Default();

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
};

DEFINE_REFCOUNTED_TYPE(TCoordinatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TApiConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration BanCacheExpirationTime;
    int ConcurrencyLimit;

    bool DisableCorsCheck;

    bool ForceTracing;

    TApiConfig()
    {
        RegisterParameter("ban_cache_expiration_time", BanCacheExpirationTime)
            .Default(TDuration::Seconds(60));

        RegisterParameter("concurrency_limit", ConcurrencyLimit)
            .Default(1024);

        RegisterParameter("disable_cors_check", DisableCorsCheck)
            .Default(false);

        RegisterParameter("force_tracing", ForceTracing)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TApiConfig)

////////////////////////////////////////////////////////////////////////////////

class TClickHouseConfig
    : public NYTree::TYsonSerializable
{
public:
    TString DiscoveryPath;
    NHttp::TClientConfigPtr HttpClient;
    TDuration ProfilingPeriod;

    TClickHouseConfig()
    {
        RegisterParameter("discovery_path", DiscoveryPath)
            .Default("//sys/clickhouse/cliques");
        RegisterParameter("http_client", HttpClient)
            .DefaultNew();
        RegisterParameter("profiling_period", ProfilingPeriod)
            .Default(TDuration::Seconds(1));

        RegisterPreprocessor([&] {
            HttpClient->HeaderReadTimeout = TDuration::Hours(1);
            HttpClient->BodyReadIdleTimeout = TDuration::Hours(1);
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TClickHouseConfig)

////////////////////////////////////////////////////////////////////////////////

class TProxyConfig
    : public TServerConfig
{
public:
    int Port;
    int ThreadCount;

    NHttp::TServerConfigPtr HttpServer;
    NHttps::TServerConfigPtr HttpsServer;
    NYTree::INodePtr Driver;

    NAuth::TAuthenticationManagerConfigPtr Auth;

    TCoordinatorConfigPtr Coordinator;
    TApiConfigPtr Api;

    TClickHouseConfigPtr ClickHouse;

    TString UIRedirectUrl;

    NYTree::IMapNodePtr CypressAnnotations;

    TProxyConfig()
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
    }
};

DEFINE_REFCOUNTED_TYPE(TProxyConfig)

////////////////////////////////////////////////////////////////////////////////

NYTree::INodePtr ConvertFromLegacyConfig(const NYTree::INodePtr& legacyConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
