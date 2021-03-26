#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/rpc_proxy/config.h>

#include <yt/yt/ytlib/auth/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/security_client/config.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/formats/public.h>

#include <yt/yt/core/misc/config.h>

#include <yt/yt/core/rpc/grpc/config.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TSecurityManagerConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TAsyncExpiringCacheConfigPtr UserCache;

    TSecurityManagerConfig()
    {
        RegisterParameter("user_cache", UserCache)
            .DefaultNew();

        RegisterPreprocessor([&] {
            UserCache->ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(60);
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TSecurityManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TApiServiceConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    bool VerboseLogging;
    bool EnableModifyRowsRequestReordering;
    bool ForceTracing;

    TSlruCacheConfigPtr ClientCache;

    i64 ReadBufferRowCount;
    i64 ReadBufferDataWeight;

    TSecurityManagerConfigPtr SecurityManager;

    TApiServiceConfig()
    {
        RegisterParameter("verbose_logging", VerboseLogging)
            .Default(false);
        RegisterParameter("enable_modify_rows_request_reordering", EnableModifyRowsRequestReordering)
            .Default(true);
        RegisterParameter("force_tracing", ForceTracing)
            .Default(false);
        RegisterParameter("client_cache", ClientCache)
            .Default(New<TSlruCacheConfig>(1000));
        RegisterParameter("read_buffer_row_count", ReadBufferRowCount)
            .Default(10000);
        RegisterParameter("read_buffer_data_weight", ReadBufferDataWeight)
            .Default(16_MB);
        RegisterParameter("security_manager", SecurityManager)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TApiServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryServiceConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    bool Enable;
    TDuration LivenessUpdatePeriod;
    TDuration ProxyUpdatePeriod;
    TDuration AvailabilityPeriod;
    TDuration BackoffPeriod;

    TDiscoveryServiceConfig()
    {
        RegisterParameter("enable", Enable)
            .Default(true);
        RegisterParameter("liveness_update_period", LivenessUpdatePeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("proxy_update_period", ProxyUpdatePeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("availability_period", AvailabilityPeriod)
            .Default(TDuration::Seconds(15))
            .GreaterThan(LivenessUpdatePeriod);
        RegisterParameter("backoff_period", BackoffPeriod)
            .Default(TDuration::Seconds(60))
            .GreaterThan(AvailabilityPeriod);
    }
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TAccessCheckerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Whether access checker is enabled.
    bool Enabled;

    //! Access checker will check use permission for
    //! PathPrefix/ProxyRole path.
    TString PathPrefix;

    //! Whether user should be allowed to use proxy
    //! if PathPrefix/ProxyRole does not exist.
    bool AllowAccessIfNodeDoesNotExist;

    //! Parameters of the permission cache.
    NSecurityClient::TPermissionCacheConfigPtr Cache;

    TAccessCheckerConfig()
    {
        RegisterParameter("enabled", Enabled)
            .Default(false);

        RegisterParameter("path_prefix", PathPrefix)
            .Default("//sys/rpc_proxy_roles");

        RegisterParameter("allow_access_if_node_does_not_exist", AllowAccessIfNodeDoesNotExist)
            .Default(true);

        RegisterParameter("cache", Cache)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TAccessCheckerConfig)

////////////////////////////////////////////////////////////////////////////////

class TAccessCheckerDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Whether access checker is enabled.
    std::optional<bool> Enabled;

    TAccessCheckerDynamicConfig()
    {
        RegisterParameter("enabled", Enabled)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TAccessCheckerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TProxyConfig
    : public TServerConfig
    , public NAuth::TAuthenticationManagerConfig
{
public:
    //! Proxy-to-master connection.
    NApi::NNative::TConnectionConfigPtr ClusterConnection;
    TApiServiceConfigPtr ApiService;
    TDiscoveryServiceConfigPtr DiscoveryService;
    //! Known RPC proxy addresses.
    NNodeTrackerClient::TNetworkAddressList Addresses;
    int WorkerThreadPoolSize;

    TAccessCheckerConfigPtr AccessChecker;

    //! GRPC server configuration.
    NRpc::NGrpc::TServerConfigPtr GrpcServer;

    NYTree::IMapNodePtr CypressAnnotations;

    bool AbortOnUnrecognizedOptions;
    //! For testing purposes.
    bool RetryRequestQueueSizeLimitExceeded;

    TProxyConfig()
    {
        RegisterParameter("cluster_connection", ClusterConnection);

        RegisterParameter("grpc_server", GrpcServer)
            .Default();
        RegisterParameter("api_service", ApiService)
            .DefaultNew();
        RegisterParameter("discovery_service", DiscoveryService)
            .DefaultNew();
        RegisterParameter("addresses", Addresses)
            .Default();
        RegisterParameter("worker_thread_pool_size", WorkerThreadPoolSize)
            .GreaterThan(0)
            .Default(8);

        RegisterParameter("access_checker", AccessChecker)
            .DefaultNew();

        RegisterParameter("cypress_annotations", CypressAnnotations)
            .Default(NYTree::BuildYsonNodeFluently()
                .BeginMap()
                .EndMap()
            ->AsMap());

        RegisterParameter("abort_on_unrecognized_options", AbortOnUnrecognizedOptions)
            .Default(false);

        RegisterParameter("retry_request_queue_size_limit_exceeded", RetryRequestQueueSizeLimitExceeded)
            .Default(true);

        RegisterPostprocessor([&] {
            if (GrpcServer && GrpcServer->Addresses.size() > 1) {
                THROW_ERROR_EXCEPTION("Multiple GRPC addresses are not supported");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TProxyConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicProxyConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    NTracing::TSamplingConfigPtr Tracing;
    THashMap<NFormats::EFormatType, TFormatConfigPtr> Formats;

    TAccessCheckerDynamicConfigPtr AccessChecker;

    TDynamicProxyConfig()
    {
        RegisterParameter("tracing", Tracing)
            .DefaultNew();
        RegisterParameter("formats", Formats)
            .Default();

        RegisterParameter("access_checker", AccessChecker)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicProxyConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
