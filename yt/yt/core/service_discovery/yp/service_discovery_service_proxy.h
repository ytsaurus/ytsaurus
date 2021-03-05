#pragma once

#include <infra/yp_service_discovery/api/api.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NServiceDiscovery::NYP {

////////////////////////////////////////////////////////////////////////////////

class TServiceDiscoveryServiceProxy
    : public NYT::NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TServiceDiscoveryServiceProxy, TServiceDiscoveryService,
        .SetNamespace("NYP.NServiceDiscovery.NApi"));

    DEFINE_RPC_PROXY_METHOD(::NYP::NServiceDiscovery::NApi, ResolveEndpoints);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServiceDiscovery::NYP
