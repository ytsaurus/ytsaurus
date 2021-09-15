#pragma once

#include <yt/yt/ytlib/data_node_tracker_client/proto/data_node_tracker_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NDataNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

class TDataNodeTrackerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TDataNodeTrackerServiceProxy, DataNodeTrackerService,
        .SetAcceptsBaggage(false));

    DEFINE_RPC_PROXY_METHOD(NProto, FullHeartbeat);
    DEFINE_RPC_PROXY_METHOD(NProto, IncrementalHeartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNodeTrackerClient
