#pragma once

#include "public.h"

#include <yt/server/misc/config.h>

#include <yt/ytlib/auth/config.h>

#include <yt/server/rpc_proxy/config.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/api/native/config.h>

#include <yt/core/rpc/grpc/config.h>

namespace NYT {
namespace NCellProxy {

////////////////////////////////////////////////////////////////////////////////

class TCellProxyConfig
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

    TCellProxyConfig()
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

        RegisterPostprocessor([&] {
            ClusterConnection->ThreadPoolSize = Null;

            if (GrpcServer && GrpcServer->Addresses.size() > 1) {
                THROW_ERROR_EXCEPTION("Multiple GRPC addresses are not supported");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TCellProxyConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellProxy
} // namespace NYT
