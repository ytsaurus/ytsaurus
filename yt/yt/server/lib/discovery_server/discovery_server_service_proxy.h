#pragma once

#include <yt/core/rpc/client.h>

#include <yt/server/lib/discovery_server/proto/discovery_server_service.pb.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryServerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TDiscoveryServerServiceProxy, DiscoveryServerService);

    DEFINE_RPC_PROXY_METHOD(NProto, ProcessGossip);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer
