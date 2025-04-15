#pragma once

#include "public.h"

#include <yt/yt/server/lib/rpc_proxy/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/cypress_registrar/public.h>

#include <yt/yt/server/lib/signature/public.h>

#include <yt/yt/library/auth_server/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/security_client/config.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/formats/public.h>

#include <yt/yt/core/misc/config.h>

#include <yt/yt/core/rpc/grpc/config.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/library/dynamic_config/config.h>

#include <yt/yt/library/program/config.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

struct TDiscoveryServiceConfig
    : public virtual NYTree::TYsonStruct
{
    bool Enable;
    TDuration LivenessUpdatePeriod;
    TDuration ProxyUpdatePeriod;
    TDuration AvailabilityPeriod;
    TDuration BackoffPeriod;

    TCypressRegistrarConfigPtr CypressRegistrar;

    REGISTER_YSON_STRUCT(TDiscoveryServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryServiceConfig)

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

struct TProxyMemoryLimits
    : public NYTree::TYsonStruct
{
    std::optional<i64> Total;
    std::optional<i64> HeavyRequest;

    REGISTER_YSON_STRUCT(TProxyMemoryLimits);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyMemoryLimits)

////////////////////////////////////////////////////////////////////////////////

struct TProxyBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
    , public NAuth::TAuthenticationManagerConfig
{
    //! Initial config for API service.
    TApiServiceConfigPtr ApiService;

    TDiscoveryServiceConfigPtr DiscoveryService;
    //! Known RPC proxy addresses.
    NNodeTrackerClient::TNetworkAddressList Addresses;
    int WorkerThreadPoolSize;

    TAccessCheckerConfigPtr AccessChecker;

    //! GRPC server configuration.
    NRpc::NGrpc::TServerConfigPtr GrpcServer;

    NAuth::TAuthenticationManagerConfigPtr TvmOnlyAuth;

    NYTree::IMapNodePtr CypressAnnotations;

    bool AbortOnUnrecognizedOptions;
    //! For testing purposes.
    bool RetryRequestQueueSizeLimitExceeded;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;

    // COMPAT(gritukan): Drop it after migration to tagged configs.
    TString DynamicConfigPath;
    bool UseTaggedDynamicConfig;

    std::string Role;

    TProxyMemoryLimitsPtr MemoryLimits;

    bool EnableShuffleService;

    THeapProfilerConfigPtr HeapProfiler;

    NSignature::TSignatureGenerationConfigPtr SignatureGeneration;
    NSignature::TSignatureValidationConfigPtr SignatureValidation;

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

struct TProxyDynamicConfig
    : public TSingletonsDynamicConfig
{
    TApiServiceDynamicConfigPtr Api;

    NTracing::TSamplerConfigPtr Tracing;

    TAccessCheckerDynamicConfigPtr AccessChecker;

    NApi::NNative::TConnectionDynamicConfigPtr ClusterConnection;

    NBus::TBusServerDynamicConfigPtr BusServer;
    NRpc::TServerDynamicConfigPtr RpcServer;

    TProxyMemoryLimitsPtr MemoryLimits;

    REGISTER_YSON_STRUCT(TProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TBundleProxyDynamicConfig
    : public NYTree::TYsonStruct
{
    std::optional<NObjectClient::TCellTag> ClockClusterTag;

    REGISTER_YSON_STRUCT(TBundleProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
