#pragma once

#include <yp/client/nodes/proto/node_tracker_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYP {
namespace NClient {
namespace NNodes {

////////////////////////////////////////////////////////////////////////////////

class TNodeTrackerServiceProxy
    : public NYT::NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TNodeTrackerServiceProxy, NodeTrackerService,
        .SetNamespace("NYP.NClient.NNodes.NProto"));

    DEFINE_RPC_PROXY_METHOD(NProto, Handshake);
    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodes
} // namespace NClient
} // namespace NYP
