#pragma once

#include <yt/ytlib/tablet_node_tracker_client/proto/tablet_node_tracker_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT::NTabletNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

class TTabletNodeTrackerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTabletNodeTrackerServiceProxy, TabletNodeTrackerService);

    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNodeTrackerClient
