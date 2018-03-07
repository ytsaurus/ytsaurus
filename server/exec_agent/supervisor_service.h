#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/server/exec_agent/supervisor_service.pb.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TSupervisorService
    : public NRpc::TServiceBase
{
public:
    explicit TSupervisorService(NCellNode::TBootstrap* bootstrap);

private:
    NCellNode::TBootstrap* const Bootstrap;

    DECLARE_RPC_SERVICE_METHOD(NProto, GetJobSpec);
    DECLARE_RPC_SERVICE_METHOD(NProto, OnJobFinished);
    DECLARE_RPC_SERVICE_METHOD(NProto, OnJobPrepared);
    DECLARE_RPC_SERVICE_METHOD(NProto, OnJobProgress);
    DECLARE_RPC_SERVICE_METHOD(NProto, UpdateResourceUsage);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

