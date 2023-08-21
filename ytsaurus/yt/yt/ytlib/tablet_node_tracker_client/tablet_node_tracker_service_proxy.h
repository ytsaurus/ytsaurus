#pragma once

#include <yt/yt/ytlib/tablet_node_tracker_client/proto/tablet_node_tracker_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NTabletNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

class TTabletNodeTrackerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTabletNodeTrackerServiceProxy, TabletNodeTrackerService,
        .SetAcceptsBaggage(false));

    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNodeTrackerClient
