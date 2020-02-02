#pragma once

#include <yt/ytlib/tablet_cell_client/proto/tablet_cell_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT::NTabletCellClient {

////////////////////////////////////////////////////////////////////////////////

class TTabletCellServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTabletCellServiceProxy, TTabletCellService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NProto, RequestHeartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletCellClient

