#pragma once

#include <yt/yt/ytlib/tablet_cell_client/proto/tablet_cell_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NTabletCellClient {

////////////////////////////////////////////////////////////////////////////////

// COMPAT(babenko): drop in 22.2
class TTabletCellServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTabletCellServiceProxy, TTabletCellService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NProto, RequestHeartbeat);
};

class TTabletCellServiceProxyFixedSpelling
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTabletCellServiceProxyFixedSpelling, TabletCellService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NProto, RequestHeartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletCellClient

