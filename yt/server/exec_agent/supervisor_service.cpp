#include "stdafx.h"
#include "supervisor_service.h"
#include "supervisor_service_proxy.h"
#include "job_manager.h"
#include "job.h"
#include "bootstrap.h"
#include "private.h"

#include <server/scheduler/job_resources.h>

namespace NYT {
namespace NExecAgent {

using namespace NScheduler;

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
    RegisterMethod(
        RPC_SERVICE_METHOD_DESC(GetJobSpec)
        .SetResponseHeavy(true));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(OnJobFinished));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(OnJobProgress)
        .SetOneWay(true));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdateResourceUtilization)
        .SetOneWay(true));
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
    auto error = FromProto(request->result().error());
    context->SetRequestInfo("JobId: %s, Error: %s",
        ~jobId.ToString(),
        ~ToString(error));

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

DEFINE_ONE_WAY_RPC_SERVICE_METHOD(TSupervisorService, UpdateResourceUtilization)
{
    auto jobId = TJobId::FromProto(request->job_id());
    const auto& utilization = request->utilization();

    context->SetRequestInfo("JobId: %s, Utilization: {%s}",
        ~jobId.ToString(),
        ~FormatResources(utilization));

    auto job = Bootstrap->GetJobManager()->GetJob(jobId);
    job->UpdateResourceUtilization(utilization);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
