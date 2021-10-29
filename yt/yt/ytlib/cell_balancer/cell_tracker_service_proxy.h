#pragma once

#include <yt/yt/ytlib/cell_balancer/proto/cell_tracker_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NCellBalancerClient {

////////////////////////////////////////////////////////////////////////////////

class TCellTrackerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TCellTrackerServiceProxy, CellTrackerService,
        .SetAcceptsBaggage(false));

    DEFINE_RPC_PROXY_METHOD(NProto, ReassignPeers);
    DEFINE_RPC_PROXY_METHOD(NProto, GetClusterState);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancerClient
