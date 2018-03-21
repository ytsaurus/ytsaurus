#pragma once

#include <yp/client/nodes/proto/agent_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYP {
namespace NClient {
namespace NNodes {

////////////////////////////////////////////////////////////////////////////////

class TAgentServiceProxy
    : public NYT::NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TAgentServiceProxy, AgentService,
        .SetNamespace("NYP.NClient.NNodes.NProto"));

    DEFINE_RPC_PROXY_METHOD(NProto, Notify);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodes
} // namespace NClient
} // namespace NYP
