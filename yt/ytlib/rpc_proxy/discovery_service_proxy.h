#pragma once

#include "public.h"

#include <yt/ytlib/rpc_proxy/proto/discovery_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TDiscoveryServiceProxy, DiscoveryService,
        .SetProtocolVersion(0));

    DEFINE_RPC_PROXY_METHOD(NRpcProxy::NProto, DiscoverProxies,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
