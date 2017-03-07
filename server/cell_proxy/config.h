#pragma once

#include "public.h"

#include <yt/server/misc/config.h>

#include <yt/server/blackbox/config.h>

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
    NBlackbox::TTokenAuthenticatorConfigPtr TokenAuthenticator;

    TCellProxyConfig()
    {
        RegisterParameter("cluster_connection", ClusterConnection);

        RegisterParameter("blackbox", Blackbox)
            .DefaultNew();
        RegisterParameter("cookie_authenticator", CookieAuthenticator)
            .DefaultNew();
        RegisterParameter("token_authenticator", TokenAuthenticator)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TCellProxyConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellProxy
} // namespace NYT
