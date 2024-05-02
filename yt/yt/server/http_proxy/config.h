#pragma once

#include "private.h"
#include "component_discovery.h"

#include <yt/yt/server/http_proxy/clickhouse/public.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/zookeeper_proxy/public.h>

#include <yt/yt/server/lib/cypress_registrar/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/library/dynamic_config/config.h>

#include <yt/yt/library/auth_server/public.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

#include <yt/yt/library/profiling/solomon/proxy.h>

#include <yt/yt/client/driver/public.h>

#include <yt/yt/client/formats/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/https/public.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

class TYTComponentEndpointProviderConfig
    : public NYTree::TYsonStruct
{
public:
    EYTComponentType ComponentType;
    //! This monitoring port will be used with all hosts discovered for the configured component.
    int MonitoringPort;
    //! A list of native solomon shards names to pull.
    //! The endpoint provider will produce a separate endpoint with each shard and a corresponding instance tag for each discovered host.
    //! Defaults to the `all` shard.
    std::vector<TString> Shards;

    //! If set to true, instance names will contain the discovered main port of the corresponding service.
    //! Set to false by default. Mostly useful in tests, real clusters have anti-affinity rules.
    bool IncludePortInInstanceName;

    //! Duration after which a HTTP proxy is considered offline by the discovery mechanism.
    TDuration ProxyDeathAge;

    REGISTER_YSON_STRUCT(TYTComponentEndpointProviderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYTComponentEndpointProviderConfig)

////////////////////////////////////////////////////////////////////////////////

//! TODO(achulkov2): Dynamic config.
class TSolomonProxyConfig
    : public NProfiling::TSolomonProxyConfig
{
public:
    //! No endpoints are specified by default, which leads to an empty result.
    std::vector<TYTComponentEndpointProviderConfigPtr> EndpointProviders;

    REGISTER_YSON_STRUCT(TSolomonProxyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSolomonProxyConfig)

////////////////////////////////////////////////////////////////////////////////

class TCoordinatorConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;
    bool Announce;

    std::optional<TString> PublicFqdn;
    std::optional<TString> DefaultRoleFilter;

    TDuration HeartbeatInterval;
    TDuration DeathAge;
    TDuration CypressTimeout;

    bool ShowPorts;

    double LoadAverageWeight;
    double NetworkLoadWeight;
    double RandomnessWeight;
    double DampeningWeight;

    TCypressRegistrarConfigPtr CypressRegistrar;

    REGISTER_YSON_STRUCT(TCoordinatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCoordinatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TDelayBeforeCommand
    : public NYTree::TYsonStruct
{
public:
    TDuration Delay;
    TString ParameterPath;
    TString Substring;

    REGISTER_YSON_STRUCT(TDelayBeforeCommand);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TDelayBeforeCommand)

class TApiTestingOptions
    : public NYTree::TYsonStruct
{
public:
    THashMap<TString, TIntrusivePtr<TDelayBeforeCommand>> DelayBeforeCommand;

    THeapProfilerTestingOptionsPtr HeapProfiler;

    REGISTER_YSON_STRUCT(TApiTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TApiTestingOptions)

////////////////////////////////////////////////////////////////////////////////

class TFramingConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;
    std::optional<TDuration> KeepAlivePeriod;

    REGISTER_YSON_STRUCT(TFramingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFramingConfig)

////////////////////////////////////////////////////////////////////////////////

class TApiConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration BanCacheExpirationTime;
    int ConcurrencyLimit;

    NHttp::TCorsConfigPtr Cors;

    bool ForceTracing;

    TApiTestingOptionsPtr TestingOptions;

    REGISTER_YSON_STRUCT(TApiConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TApiConfig)

////////////////////////////////////////////////////////////////////////////////

class TApiDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    TFramingConfigPtr Framing;

    THashMap<NFormats::EFormatType, TFormatConfigPtr> Formats;

    bool EnableAllocationTags;

    REGISTER_YSON_STRUCT(TApiDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TApiDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TAccessCheckerConfig
    : public NYTree::TYsonStruct
{
public:
    //! Whether access checker is enabled.
    bool Enabled;

    //! Access checker will check use permission for
    //! PathPrefix/ProxyRole path or
    //! PathPrefix/ProxyRole/principal if UseAccessControlObjects is set.
    TString PathPrefix;

    bool UseAccessControlObjects;

    //! Parameters of the permission cache.
    NSecurityClient::TPermissionCacheConfigPtr Cache;

    REGISTER_YSON_STRUCT(TAccessCheckerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAccessCheckerConfig)

////////////////////////////////////////////////////////////////////////////////

class TAccessCheckerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    //! Whether access checker is enabled.
    std::optional<bool> Enabled;

    REGISTER_YSON_STRUCT(TAccessCheckerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAccessCheckerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TProxyConfig
    : public TNativeServerConfig
{
public:
    int Port;
    int ThreadCount;

    NHttp::TServerConfigPtr HttpServer;
    NHttps::TServerConfigPtr HttpsServer;
    NHttp::TServerConfigPtr TvmOnlyHttpServer;
    NHttps::TServerConfigPtr TvmOnlyHttpsServer;
    NDriver::TDriverConfigPtr Driver;

    NAuth::TAuthenticationManagerConfigPtr Auth;
    NAuth::TAuthenticationManagerConfigPtr TvmOnlyAuth;

    bool RetryRequestQueueSizeLimitExceeded;

    TString Role;

    TCoordinatorConfigPtr Coordinator;
    TApiConfigPtr Api;

    TAccessCheckerConfigPtr AccessChecker;

    NClickHouse::TStaticClickHouseConfigPtr ClickHouse;

    TString UIRedirectUrl;

    NYTree::IMapNodePtr CypressAnnotations;

    bool AbortOnUnrecognizedOptions;

    TString DefaultNetwork;
    THashMap<TString, std::vector<NNet::TIP6Network>> Networks;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;

    // COMPAT(gritukan): Drop it after migration to tagged configs.
    TString DynamicConfigPath;
    bool UseTaggedDynamicConfig;

    // COMPAT(danilalexeev): unretriable read-only error.
    bool RetryReadOnlyResponseError;

    NZookeeperProxy::TZookeeperProxyConfigPtr ZookeeperProxy;

    //! Configuration for solomon proxy, which allows collecting merged metrics from other YT components through HTTP proxies.
    TSolomonProxyConfigPtr SolomonProxy;

    REGISTER_YSON_STRUCT(TProxyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyConfig)

////////////////////////////////////////////////////////////////////////////////

// TDynamicConfig is part of proxy configuration stored in cypress.
//
// NOTE: config might me unavalable. Users must handle such cases
// gracefully.
class TProxyDynamicConfig
    : public TNativeSingletonsDynamicConfig
{
public:
    TApiDynamicConfigPtr Api;

    NTracing::TSamplerConfigPtr Tracing;

    TString FitnessFunction;
    double CpuWeight;
    double CpuWaitWeight;
    double ConcurrentRequestsWeight;

    bool RelaxCsrfCheck;

    NClickHouse::TDynamicClickHouseConfigPtr ClickHouse;

    TAccessCheckerDynamicConfigPtr AccessChecker;

    // COMPAT(gritukan, levysotsky)
    TFramingConfigPtr Framing;
    THashMap<NFormats::EFormatType, TFormatConfigPtr> Formats;

    NApi::NNative::TConnectionDynamicConfigPtr ClusterConnection;

    REGISTER_YSON_STRUCT(TProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
