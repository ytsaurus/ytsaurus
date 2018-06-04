#pragma once

#include "public.h"

#include <yt/server/misc/config.h>

#include <yt/ytlib/auth/config.h>

#include <yt/server/rpc_proxy/config.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/api/config.h>

namespace NYT {
namespace NCellProxy {

////////////////////////////////////////////////////////////////////////////////

class TCellProxyConfig
    : public TServerConfig
{
public:
    //! Proxy-to-master connection.
    NApi::TNativeConnectionConfigPtr ClusterConnection;
    NAuth::TDefaultBlackboxServiceConfigPtr Blackbox;
    NAuth::TBlackboxCookieAuthenticatorConfigPtr CookieAuthenticator;
    NAuth::TCachingBlackboxTokenAuthenticatorConfigPtr TokenAuthenticator;
    NRpcProxy::TApiServiceConfigPtr ApiService;
    NRpcProxy::TDiscoveryServiceConfigPtr DiscoveryService;
    //! Known RPC proxy addresses.
    NNodeTrackerClient::TNetworkAddressList Addresses;
    int WorkerThreadPoolSize;
    //! Switch off for local mode and testing purposes only.
    //! If enabled, every call is considered to be invoked as root.
    bool EnableAuthentication;

    TCellProxyConfig()
    {
        RegisterParameter("cluster_connection", ClusterConnection);

        RegisterParameter("blackbox", Blackbox)
            .DefaultNew();
        RegisterParameter("cookie_authenticator", CookieAuthenticator)
            .DefaultNew();
        RegisterParameter("token_authenticator", TokenAuthenticator)
            .DefaultNew();
        RegisterParameter("api_service", ApiService)
            .DefaultNew();
        RegisterParameter("discovery_service", DiscoveryService)
            .DefaultNew();
        RegisterParameter("addresses", Addresses)
            .Default();
        RegisterParameter("worker_thread_pool_size", WorkerThreadPoolSize)
            .GreaterThan(0)
            .Default(8);
        RegisterParameter("enable_authentication", EnableAuthentication)
            .Default(true);

        RegisterPostprocessor([&] {
            ClusterConnection->ThreadPoolSize = Null;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TCellProxyConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellProxy
} // namespace NYT
