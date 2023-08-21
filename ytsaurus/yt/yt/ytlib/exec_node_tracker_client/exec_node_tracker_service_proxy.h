#pragma once

#include <yt/yt/ytlib/exec_node_tracker_client/proto/exec_node_tracker_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NExecNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

class TExecNodeTrackerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TExecNodeTrackerServiceProxy, ExecNodeTrackerService,
        .SetAcceptsBaggage(false));

    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNodeTrackerClient
