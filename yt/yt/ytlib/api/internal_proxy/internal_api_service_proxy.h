#pragma once

#include <yt/yt/ytlib/tablet_client/proto/master_tablet_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NApi::NInternalProxy {

////////////////////////////////////////////////////////////////////////////////

class TInternalApiServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TInternalApiServiceProxy, InternalApiService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NTabletClient::NProto, GetTableBalancingAttributes);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NInternalProxy
