#pragma once

#include <yt/yt/ytlib/cellar_node_tracker_client/proto/cellar_node_tracker_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NCellarNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

class TCellarNodeTrackerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TCellarNodeTrackerServiceProxy, CellarNodeTrackerService);

    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNodeTrackerClient
