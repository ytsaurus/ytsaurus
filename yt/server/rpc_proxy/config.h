#pragma once

#include "public.h"

#include <yt/client/api/config.h>

#include <yt/server/lib/misc/config.h>

#include <yt/ytlib/auth/config.h>

#include <yt/server/rpc_proxy/config.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/api/native/config.h>

#include <yt/core/misc/config.h>

#include <yt/core/rpc/grpc/config.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TDynamicConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    NTracing::TSamplingConfigPtr Tracing;

    TDynamicConfig()
    {
        RegisterParameter("tracing", Tracing)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicConfig)

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
            .Default((i64) 10000);
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
    TDuration LivenessUpdatePeriod;
    TDuration ProxyUpdatePeriod;
    TDuration AvailabilityPeriod;
    TDuration BackoffPeriod;

    TDiscoveryServiceConfig()
    {
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

class TRpcProxyConfig
    : public TServerConfig
    , public NAuth::TAuthenticationManagerConfig
{
public:
    //! Proxy-to-master connection.
    NApi::NNative::TConnectionConfigPtr ClusterConnection;
    NRpcProxy::TApiServiceConfigPtr ApiService;
    NRpcProxy::TDiscoveryServiceConfigPtr DiscoveryService;
    //! Known RPC proxy addresses.
    NNodeTrackerClient::TNetworkAddressList Addresses;
    int WorkerThreadPoolSize;

    //! GRPC server configuration.
    NRpc::NGrpc::TServerConfigPtr GrpcServer;

    NYTree::IMapNodePtr CypressAnnotations;

    bool AbortOnUnrecognizedOptions;
    //! For testing purposes.
    bool RetryRequestQueueSizeLimitExceeded;

    TRpcProxyConfig()
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
            ClusterConnection->ThreadPoolSize = std::nullopt;

            if (GrpcServer && GrpcServer->Addresses.size() > 1) {
                THROW_ERROR_EXCEPTION("Multiple GRPC addresses are not supported");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TRpcProxyConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
