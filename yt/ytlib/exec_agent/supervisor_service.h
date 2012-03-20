#pragma once

#include "public.h"
#include "supervisor_service.pb.h"

#include <ytlib/rpc/server.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TSupervisorService
    : public NRpc::TServiceBase
{
public:
    typedef TIntrusivePtr<TSupervisorService> TPtr;

    TSupervisorService(TBootstrap* bootstrap);

private:
    typedef TSupervisorService TThis;

    TBootstrap* Bootstrap;

    DECLARE_RPC_SERVICE_METHOD(NProto, GetJobSpec);
    DECLARE_ONE_WAY_RPC_SERVICE_METHOD(NProto, OnJobFinished);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

