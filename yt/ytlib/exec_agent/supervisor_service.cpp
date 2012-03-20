#include "stdafx.h"

#include "supervisor_service.h"
#include "supervisor_service_proxy.h"
#include "job_manager.h"

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TSupervisorService::TSupervisorService(
    NRpc::IServer* server,
    TJobManager* jobManager)
    : NRpc::TServiceBase(
        ~jobManager->GetInvoker(),
        TSupervisorServiceProxy::GetServiceName(),
        Logger.GetCategory())
    //    , Config(config)
    , JobManager(jobManager)
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobSpec));
    RegisterMethod(ONE_WAY_RPC_SERVICE_METHOD_DESC(OnJobFinished));

    server->RegisterService(this);
}

DEFINE_RPC_SERVICE_METHOD(TSupervisorService, GetJobSpec)
{
    //ToDo: context->SetRequestInfo("ChunkId: %s", ~chunkId.ToString());

    *(response->mutable_job_spec()) = JobManager->GetJobSpec(
        TJobId::FromProto(request->job_id()));

    context->Reply();
}

DEFINE_ONE_WAY_RPC_SERVICE_METHOD(TSupervisorService, OnJobFinished)
{
    //ToDo: context->SetRequestInfo("ChunkId: %s", ~chunkId.ToString());

    JobManager->SetJobResult(
        TJobId::FromProto(request->job_id()),
        request->job_result());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
