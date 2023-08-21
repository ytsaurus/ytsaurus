#pragma once

#include "public.h"

#include <yt/yt/server/lib/exec_node/proto/supervisor_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TSupervisorServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TSupervisorServiceProxy, SupervisorService,
        .SetProtocolVersion(4));

    DEFINE_RPC_PROXY_METHOD(NProto, GetJobSpec);
    DEFINE_RPC_PROXY_METHOD(NProto, OnJobProxySpawned);
    DEFINE_RPC_PROXY_METHOD(NProto, PrepareArtifact);
    DEFINE_RPC_PROXY_METHOD(NProto, OnArtifactPreparationFailed);
    DEFINE_RPC_PROXY_METHOD(NProto, OnArtifactsPrepared);
    DEFINE_RPC_PROXY_METHOD(NProto, OnJobFinished);
    DEFINE_RPC_PROXY_METHOD(NProto, OnJobPrepared);
    DEFINE_RPC_PROXY_METHOD(NProto, OnJobProgress);
    DEFINE_RPC_PROXY_METHOD(NProto, UpdateResourceUsage);
    DEFINE_RPC_PROXY_METHOD(NProto, ThrottleJob);
    DEFINE_RPC_PROXY_METHOD(NProto, PollThrottlingRequest);
    DEFINE_RPC_PROXY_METHOD(NProto, OnJobMemoryThrashing);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
