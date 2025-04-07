#pragma once

#include "private.h"
#include "component_discovery.h"

#include <yt/yt/server/http_proxy/clickhouse/public.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/cypress_registrar/public.h>

#include <yt/yt/server/lib/signature/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/library/dynamic_config/config.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/library/auth_server/public.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

#include <yt/yt/library/profiling/solomon/proxy.h>

#include <yt/yt/library/program/config.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/client/driver/public.h>

#include <yt/yt/client/formats/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/https/public.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

//! Provides endpoints used for proxying metrics from internal cluster components.
struct TProfilingEndpointProviderConfig
    : public NYTree::TYsonStruct
{
    EClusterComponentType ComponentType;
    //! This monitoring port will be used with all hosts discovered for the configured component.
    int MonitoringPort;
    //! A list of native solomon shards names to pull.
    //! The endpoint provider will produce a separate endpoint with each shard and a corresponding instance tag for each discovered host.
    //! Defaults to the `all` shard.
    std::vector<TString> Shards;

    //! If set to true, instance names will contain the discovered main port of the corresponding service.
    //! Set to false by default. Mostly useful in tests, real clusters have anti-affinity rules.
    bool IncludePortInInstanceName;

    REGISTER_YSON_STRUCT(TProfilingEndpointProviderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProfilingEndpointProviderConfig)

////////////////////////////////////////////////////////////////////////////////

//! TODO(achulkov2): Dynamic config.
struct TSolomonProxyConfig
    : public NProfiling::TSolomonProxyConfig
{
    //! No endpoints are specified by default, which leads to an empty result.
    std::vector<TProfilingEndpointProviderConfigPtr> EndpointProviders;

    REGISTER_YSON_STRUCT(TSolomonProxyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSolomonProxyConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCoordinatorConfig
    : public NYTree::TYsonStruct
{
    bool Enable;
    bool Announce;

    std::optional<TString> PublicFqdn;
    std::optional<std::string> DefaultRoleFilter;

    TDuration HeartbeatInterval;
    TDuration DeathAge;
    TDuration ReadOnlyDeathAge;
    TDuration CypressTimeout;
    TDuration OrchidTimeout;

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

    NServer::THeapProfilerTestingOptionsPtr HeapProfiler;

    REGISTER_YSON_STRUCT(TApiTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TApiTestingOptions)

////////////////////////////////////////////////////////////////////////////////

struct TFramingConfig
    : public NYTree::TYsonStruct
{
    bool Enable;
    std::optional<TDuration> KeepAlivePeriod;

    REGISTER_YSON_STRUCT(TFramingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFramingConfig)

////////////////////////////////////////////////////////////////////////////////

struct TApiConfig
    : public NYTree::TYsonStruct
{
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

struct TApiDynamicConfig
    : public NYTree::TYsonStruct
{
    TFramingConfigPtr Framing;

    THashMap<NFormats::EFormatType, NServer::TFormatConfigPtr> Formats;

    bool EnableAllocationTags;

    //! Represents the ratio of total available memory that can be utilized by each user (if user is not specified in "UserMemoryRatio"),
    //! expressed as a value between 0 and 1.
    std::optional<double> DefaultUserMemoryLimitRatio;
    THashMap<std::string, double> UserToMemoryLimitRatio;

    // COMPAT(ignat): drop the option after 25.2.
    bool UseCompressionThreadPool;

    REGISTER_YSON_STRUCT(TApiDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TApiDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TAccessCheckerConfig
    : public NYTree::TYsonStruct
{
    //! Whether access checker is enabled.
    bool Enabled;

    //! Access checker will check use permission for
    //! PathPrefix/ProxyRole path or
    //! PathPrefix/ProxyRole/principal if UseAccessControlObjects is set.
    TString PathPrefix;

    // COMPAT(verytable): Drop it after migration to aco roles everywhere.
    bool UseAccessControlObjects;

    //! Parameters of the permission cache.
    NSecurityClient::TPermissionCacheConfigPtr Cache;

    REGISTER_YSON_STRUCT(TAccessCheckerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAccessCheckerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TAccessCheckerDynamicConfig
    : public NYTree::TYsonStruct
{
    //! Whether access checker is enabled.
    std::optional<bool> Enabled;

    REGISTER_YSON_STRUCT(TAccessCheckerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAccessCheckerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TProxyMemoryLimitsConfig
    : public NYTree::TYsonStruct
{
    std::optional<i64> Total;
    std::optional<i64> HeavyRequest;

    REGISTER_YSON_STRUCT(TProxyMemoryLimitsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyMemoryLimitsConfig)

////////////////////////////////////////////////////////////////////////////////

struct TProxyBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
    int Port;
    int ThreadCount;

    NHttp::TServerConfigPtr HttpServer;
    NHttps::TServerConfigPtr HttpsServer;
    NHttp::TServerConfigPtr TvmOnlyHttpServer;
    NHttps::TServerConfigPtr TvmOnlyHttpsServer;
    NHttp::TServerConfigPtr ChytHttpServer;
    NHttps::TServerConfigPtr ChytHttpsServer;

    //! Known HTTP proxy addresses.
    NNodeTrackerClient::TNetworkAddressList Addresses;

    NDriver::TDriverConfigPtr Driver;

    NAuth::TAuthenticationManagerConfigPtr Auth;
    NAuth::TAuthenticationManagerConfigPtr TvmOnlyAuth;

    bool RetryRequestQueueSizeLimitExceeded;

    std::string Role;

    TCoordinatorConfigPtr Coordinator;
    TApiConfigPtr Api;

    TAccessCheckerConfigPtr AccessChecker;

    TProxyMemoryLimitsConfigPtr MemoryLimits;

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

    //! Configuration for solomon proxy, which allows collecting merged metrics from other YT components through HTTP proxies.
    TSolomonProxyConfigPtr SolomonProxy;

    THeapProfilerConfigPtr HeapProfiler;

    //! Configuration for signature components.
    NSignature::TSignatureValidationConfigPtr SignatureValidation;
    NSignature::TSignatureGenerationConfigPtr SignatureGeneration;

    REGISTER_YSON_STRUCT(TProxyBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

struct TProxyProgramConfig
    : public TProxyBootstrapConfig
    , public TServerProgramConfig
{
    REGISTER_YSON_STRUCT(TProxyProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyProgramConfig)

////////////////////////////////////////////////////////////////////////////////

// TDynamicConfig is part of proxy configuration stored in Cypress.
//
// NOTE: config might me unavalable. Users must handle such cases
// gracefully.
struct TProxyDynamicConfig
    : public TSingletonsDynamicConfig
{
    TApiDynamicConfigPtr Api;

    NTracing::TSamplerConfigPtr Tracing;

    TString FitnessFunction;
    double CpuWeight;
    double CpuWaitWeight;
    double ConcurrentRequestsWeight;

    bool RelaxCsrfCheck;

    NClickHouse::TDynamicClickHouseConfigPtr ClickHouse;

    TAccessCheckerDynamicConfigPtr AccessChecker;

    NBus::TBusServerDynamicConfigPtr BusServer;

    TProxyMemoryLimitsConfigPtr MemoryLimits;

    REGISTER_YSON_STRUCT(TProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
