#pragma once

#include "public.h"

#include <core/rpc/service_detail.h>

#include <server/exec_agent/supervisor_service.pb.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TSupervisorService
    : public NRpc::TServiceBase
{
public:
    explicit TSupervisorService(NCellNode::TBootstrap* bootstrap);

private:
    typedef TSupervisorService TThis;

    NCellNode::TBootstrap* Bootstrap;

    DECLARE_RPC_SERVICE_METHOD(NProto, GetJobSpec);
    DECLARE_RPC_SERVICE_METHOD(NProto, OnJobFinished);
    DECLARE_ONE_WAY_RPC_SERVICE_METHOD(NProto, OnJobProgress);
    DECLARE_ONE_WAY_RPC_SERVICE_METHOD(NProto, UpdateResourceUsage);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

