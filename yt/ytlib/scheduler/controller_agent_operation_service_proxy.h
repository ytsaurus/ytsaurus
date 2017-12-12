#pragma once

#include "public.h"

#include <yt/ytlib/scheduler/proto/controller_agent_operation_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

// TODO(ignat): move to ytlib/controller_agent
class TControllerAgentOperationServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TControllerAgentOperationServiceProxy, RPC_PROXY_DESC(ControllerAgentOperationService)
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NProto, GetOperationInfo);
    DEFINE_RPC_PROXY_METHOD(NProto, GetJobInfo);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

