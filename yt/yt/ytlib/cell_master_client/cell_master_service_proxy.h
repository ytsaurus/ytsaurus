#pragma once

#include <yt/yt/ytlib/cell_master_client/proto/cell_master_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NCellMasterClient {

////////////////////////////////////////////////////////////////////////////////

class TCellMasterServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TCellMasterServiceProxy, CellMasterService,
        .SetAcceptsBaggage(false));

    DEFINE_RPC_PROXY_METHOD(NProto, ResetDynamicallyPropagatedMasterCells);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient
