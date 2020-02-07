#pragma once

#include "private.h"
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
    std::optional<TString> DefaultRoleFilter;

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
};

DEFINE_REFCOUNTED_TYPE(TCoordinatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TApiTestingOptions
    : public NYTree::TYsonSerializable
{
public:
    class TDelayInsideGet
        : public NYTree::TYsonSerializable
    {
    public:
        TDuration Delay;
        TString Path;

        TDelayInsideGet()
        {
            RegisterParameter("delay", Delay);
            RegisterParameter("path", Path);
        }
    };
    using TDelayInsideGetPtr = TIntrusivePtr<TDelayInsideGet>;

public:
    TDelayInsideGetPtr DelayInsideGet;

    TApiTestingOptions()
    {
        RegisterParameter("delay_inside_get", DelayInsideGet)
            .Optional();
    }
};

DEFINE_REFCOUNTED_TYPE(TApiTestingOptions);
DEFINE_REFCOUNTED_TYPE(TApiTestingOptions::TDelayInsideGet);

////////////////////////////////////////////////////////////////////////////////

class TApiConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration BanCacheExpirationTime;
    int ConcurrencyLimit;

    bool DisableCorsCheck;

    bool ForceTracing;

    TDuration FramingKeepAlivePeriod;

    TApiTestingOptionsPtr TestingOptions;

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

        RegisterParameter("framing_keep_alive_period", FramingKeepAlivePeriod)
            .Default();

        RegisterParameter("testing", TestingOptions)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TApiConfig)

////////////////////////////////////////////////////////////////////////////////

class TCliqueCacheConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Base config for SlruCache.
    TSlruCacheConfigPtr CacheBase;

    //! Update of data in discovery will be scheduled if data is older than this age threshold.
    //! Update is asynchronous, current request will be processed with data from the cache.
    TDuration SoftAgeThreshold;

    //! Proxy will never use cached data if it is older than this age threshold. Will wait for update instead.
    TDuration HardAgeThreshold;

    //! Is used for updating discovery from master cache.
    TDuration MasterCacheExpireTime;

    //! How long the proxy will not send new requests to the instance after connection error to it.
    TDuration UnavailableInstanceBanTimeout;

    TCliqueCacheConfig()
    {
        RegisterParameter("cache_base", CacheBase)
            .DefaultNew(/* capacity */ 1000);
        RegisterParameter("soft_age_threshold", SoftAgeThreshold)
            .Default(TDuration::Seconds(15));
        RegisterParameter("hard_age_threshold", HardAgeThreshold)
            .Default(TDuration::Minutes(15));
        RegisterParameter("master_cache_expire_time", MasterCacheExpireTime)
            .Default(TDuration::Seconds(5));
        RegisterParameter("unavailable_instance_ban_timeout", UnavailableInstanceBanTimeout)
            .Default(TDuration::Seconds(30));
    }
};

DEFINE_REFCOUNTED_TYPE(TCliqueCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TClickHouseConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Path to folder in cypress which contains general information about all cliques.
    TString DiscoveryPath;

    NHttp::TClientConfigPtr HttpClient;
    TDuration ProfilingPeriod;

    //! Cache for cliques's discovery.
    TCliqueCacheConfigPtr CliqueCache;

    //! Prevent throwing an error if the request does not contain an authorization header.
    //! If authorization is disabled in proxy config, this flag will be set to true automatically.
    bool IgnoreMissingCredentials;

    //! How many times the proxy will retry sending the request to the randomly chosen instance
    //! when the chosen instance does not respond or respond with MovedPermanently status code.
    int DeadInstanceRetryCount;

    //! How many times the proxy can retry sending the request with old discovery from the cache.
    //! If this limit is exсeeded, next retry will be performed after force update of discovery.
    int RetryWithoutUpdateLimit;

    //! Force update can be skipped by discovery if the data is younger than this age threshold.
    TDuration ForceDiscoveryUpdateAgeThreshold;

    //! Timeout to resolve alias.
    TDuration AliasResolutionTimeout;

    //! If set to true, profiler won't wait a second to update a counter.
    //! It is useful for testing.
    bool ForceEnqueueProfiling;

    TClickHouseConfig()
    {
        RegisterParameter("discovery_path", DiscoveryPath)
            .Default("//sys/clickhouse/cliques");
        RegisterParameter("http_client", HttpClient)
            .DefaultNew();
        RegisterParameter("profiling_period", ProfilingPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("clique_cache", CliqueCache)
            .DefaultNew();
        RegisterParameter("ignore_missing_credentials", IgnoreMissingCredentials)
            .Default(false);
        RegisterParameter("dead_instance_retry_count", DeadInstanceRetryCount)
            .Default(4);
        RegisterParameter("retry_without_update_limit", RetryWithoutUpdateLimit)
            .Default(2);
        RegisterParameter("force_discovery_update_age_threshold", ForceDiscoveryUpdateAgeThreshold)
            .Default(TDuration::Seconds(1));
        RegisterParameter("alias_resolution_timeout", AliasResolutionTimeout)
            .Default(TDuration::Seconds(30));
        RegisterParameter("force_enqueue_profiling", ForceEnqueueProfiling)
            .Default(false);

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

    bool RetryRequestQueueSizeLimitExceeded;

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
    }
};

DEFINE_REFCOUNTED_TYPE(TProxyConfig)

////////////////////////////////////////////////////////////////////////////////

NYTree::INodePtr ConvertFromLegacyConfig(const NYTree::INodePtr& legacyConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
