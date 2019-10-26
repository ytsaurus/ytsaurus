#pragma once

#include <yp/client/api/proto/discovery_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYP::NClient::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryServiceProxy
    : public NYT::NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TDiscoveryServiceProxy, DiscoveryService,
        .SetNamespace("NYP.NClient.NApi.NProto"));

    DEFINE_RPC_PROXY_METHOD(NProto, GetMasters);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NNative
