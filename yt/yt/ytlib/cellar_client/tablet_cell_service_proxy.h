#pragma once

#include <yt/yt/ytlib/cellar_client/proto/tablet_cell_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NCellarClient {

////////////////////////////////////////////////////////////////////////////////

class TTabletCellServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTabletCellServiceProxy, TabletCellService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NProto, RequestHeartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarClient
