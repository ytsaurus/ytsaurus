#pragma once

#include "public.h"

#include <yt/server/exec_agent/supervisor_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TSupervisorServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TSupervisorServiceProxy, SupervisorService,
        .SetProtocolVersion(3));

    DEFINE_RPC_PROXY_METHOD(NProto, GetJobSpec);
    DEFINE_RPC_PROXY_METHOD(NProto, OnJobFinished);
    DEFINE_RPC_PROXY_METHOD(NProto, OnJobPrepared);
    DEFINE_RPC_PROXY_METHOD(NProto, OnJobProgress);
    DEFINE_RPC_PROXY_METHOD(NProto, UpdateResourceUsage);
    DEFINE_RPC_PROXY_METHOD(NProto, ThrottleJob);
    DEFINE_RPC_PROXY_METHOD(NProto, PollThrottlingRequest);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
