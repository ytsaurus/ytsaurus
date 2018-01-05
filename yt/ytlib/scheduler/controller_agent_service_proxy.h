#pragma once

#include "public.h"

#include <yt/ytlib/scheduler/proto/controller_agent_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

// TODO(ignat): move to ytlib/controller_agent
class TControllerAgentServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TControllerAgentServiceProxy, ControllerAgentService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NProto, GetOperationInfo);
    DEFINE_RPC_PROXY_METHOD(NProto, GetJobInfo);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

