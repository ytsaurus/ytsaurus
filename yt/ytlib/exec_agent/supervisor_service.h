#pragma once

#include "common.h"
#include "supervisor_service.pb.h"

#include <ytlib/rpc/server.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TJobManager;

////////////////////////////////////////////////////////////////////////////////

class TSupervisorService
    : public NRpc::TServiceBase
{
public:
    typedef TIntrusivePtr<TSupervisorService> TPtr;

    /*struct TConfig
        : public TConfigurable
    { };*/

    TSupervisorService(
        //TConfig* config,
        NRpc::IServer* server,
        TJobManager* jobManager);

private:
    typedef TSupervisorService TThis;

    DECLARE_RPC_SERVICE_METHOD(NProto, GetJobSpec);
    DECLARE_ONE_WAY_RPC_SERVICE_METHOD(NProto, OnJobFinished);

    TIntrusivePtr<TJobManager> JobManager;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

