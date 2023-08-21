#pragma once

#include "public.h"

#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>

#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

class TNodeTrackerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TNodeTrackerServiceProxy, NodeTrackerService,
        .SetProtocolVersion(14)
        .SetAcceptsBaggage(false));

    DEFINE_RPC_PROXY_METHOD(NProto, RegisterNode);
    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
