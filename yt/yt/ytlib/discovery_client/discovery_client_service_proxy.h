#pragma once

#include <yt/core/rpc/client.h>

#include <yt/ytlib/discovery_client/proto/discovery_client_service.pb.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryClientServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TDiscoveryClientServiceProxy, DiscoveryClientService);

    DEFINE_RPC_PROXY_METHOD(NProto, ListMembers);
    DEFINE_RPC_PROXY_METHOD(NProto, GetGroupMeta);

    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient

