#pragma once

#include "public.h"
#include "supervisor_service.pb.h"

#include <ytlib/rpc/server.h>
#include <ytlib/cell_node/public.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TSupervisorService
    : public NRpc::TServiceBase
{
public:
    typedef TIntrusivePtr<TSupervisorService> TPtr;

    TSupervisorService(NCellNode::TBootstrap* bootstrap);

private:
    typedef TSupervisorService TThis;

    NCellNode::TBootstrap* Bootstrap;

    DECLARE_RPC_SERVICE_METHOD(NProto, GetJobSpec);
    DECLARE_ONE_WAY_RPC_SERVICE_METHOD(NProto, OnJobFinished);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

