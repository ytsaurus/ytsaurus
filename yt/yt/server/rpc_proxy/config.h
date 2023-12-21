#pragma once

#include "public.h"

#include <yt/yt/server/lib/rpc_proxy/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/cypress_registrar/public.h>

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

#include <yt/yt/library/tracing/jaeger/sampler.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryServiceConfig
    : public virtual NYTree::TYsonStruct
{
public:
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

    // COMPAT(verytable): Drop it after migration to aco roles everywhere.
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
    , public NAuth::TAuthenticationManagerConfig
{
public:
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

    TString Role;

    REGISTER_YSON_STRUCT(TProxyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyConfig)

////////////////////////////////////////////////////////////////////////////////

class TProxyDynamicConfig
    : public TNativeSingletonsDynamicConfig
{
public:
    TApiServiceDynamicConfigPtr Api;

    NTracing::TSamplerConfigPtr Tracing;
    THashMap<NFormats::EFormatType, TFormatConfigPtr> Formats;

    TAccessCheckerDynamicConfigPtr AccessChecker;

    NApi::NNative::TConnectionDynamicConfigPtr ClusterConnection;

    REGISTER_YSON_STRUCT(TProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
