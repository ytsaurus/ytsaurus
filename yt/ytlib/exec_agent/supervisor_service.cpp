#include "stdafx.h"
#include "supervisor_service.h"
#include "supervisor_service_proxy.h"
#include "job_manager.h"
#include "job.h"
#include "bootstrap.h"
#include "private.h"

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TSupervisorService::TSupervisorService(TBootstrap* bootstrap)
    : NRpc::TServiceBase(
        bootstrap->GetControlInvoker(),
        TSupervisorServiceProxy::GetServiceName(),
        Logger.GetCategory())
    , Bootstrap(bootstrap)
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobSpec));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(OnJobFinished));
    RegisterMethod(ONE_WAY_RPC_SERVICE_METHOD_DESC(OnJobProgress));
}

DEFINE_RPC_SERVICE_METHOD(TSupervisorService, GetJobSpec)
{
    auto jobId = TJobId::FromProto(request->job_id());
    context->SetRequestInfo("JobId: %s", ~jobId.ToString());

    auto job = Bootstrap->GetJobManager()->GetJob(jobId);
    *response->mutable_job_spec() = job->GetSpec();

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TSupervisorService, OnJobFinished)
{
    auto jobId = TJobId::FromProto(request->job_id());
    auto error = TError::FromProto(request->result().error());
    context->SetRequestInfo("JobId: %s, Error: %s",
        ~jobId.ToString(),
        ~error.ToString());

    auto job = Bootstrap->GetJobManager()->GetJob(jobId);
    job->SetResult(request->result());

    context->Reply();
}

DEFINE_ONE_WAY_RPC_SERVICE_METHOD(TSupervisorService, OnJobProgress)
{
    auto jobId = TJobId::FromProto(request->job_id());

    context->SetRequestInfo("JobId: %s",
        ~jobId.ToString());

    // Progress tracking is not implemented yet.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
