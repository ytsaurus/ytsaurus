#pragma once

#include "public.h"

#include <yt/server/misc/config.h>

#include <yt/server/blackbox/config.h>

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
    NBlackbox::TDefaultBlackboxServiceConfigPtr Blackbox;
    NBlackbox::TCookieAuthenticatorConfigPtr CookieAuthenticator;
    NBlackbox::TCachingTokenAuthenticatorConfigPtr TokenAuthenticator;
    NRpcProxy::TDiscoveryServiceConfigPtr DiscoveryService;
    //! Known RPC proxy addresses.
    NNodeTrackerClient::TNetworkAddressList Addresses;

    TCellProxyConfig()
    {
        RegisterParameter("cluster_connection", ClusterConnection);

        RegisterParameter("blackbox", Blackbox)
            .DefaultNew();
        RegisterParameter("cookie_authenticator", CookieAuthenticator)
            .DefaultNew();
        RegisterParameter("token_authenticator", TokenAuthenticator)
            .DefaultNew();
        RegisterParameter("discovery_service", DiscoveryService)
            .DefaultNew();
        RegisterParameter("addresses", Addresses)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TCellProxyConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellProxy
} // namespace NYT
