#pragma once

#include "public.h"

#include <ytlib/exec_agent/supervisor_service.pb.h>
#include <ytlib/rpc/service.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TSupervisorService
    : public NRpc::TServiceBase
{
public:
    typedef TIntrusivePtr<TSupervisorService> TPtr;

    explicit TSupervisorService(TBootstrap* bootstrap);

private:
    typedef TSupervisorService TThis;

    TBootstrap* Bootstrap;

    DECLARE_RPC_SERVICE_METHOD(NProto, GetJobSpec);
    DECLARE_RPC_SERVICE_METHOD(NProto, OnJobFinished);
    DECLARE_ONE_WAY_RPC_SERVICE_METHOD(NProto, OnJobProgress);
    DECLARE_ONE_WAY_RPC_SERVICE_METHOD(NProto, OnResourceUtilizationSet);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

