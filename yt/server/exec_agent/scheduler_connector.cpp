#include "stdafx.h"
#include "scheduler_connector.h"
#include "private.h"
#include "job_manager.h"
#include "job.h"
#include "config.h"

#include <server/scheduler/job_resources.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NExecAgent {

using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& SILENT_UNUSED Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TSchedulerConnector::TSchedulerConnector(
    TSchedulerConnectorConfigPtr config,
    TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
    , ControlInvoker(bootstrap->GetControlInvoker())
    , Proxy(bootstrap->GetSchedulerChannel())
{
    YCHECK(config);
    YCHECK(bootstrap);
}

void TSchedulerConnector::Start()
{
    HeartbeatInvoker = New<TPeriodicInvoker>(
        ControlInvoker,
        BIND(&TThis::SendHeartbeat, MakeWeak(this)),
        Config->HeartbeatPeriod,
        EPeriodicInvokerMode::Manual,
        Config->HeartbeatSplay);

    // Schedule an out-of-order heartbeat whenever a job finishes
    // or its resource usage is updated.
    Bootstrap->GetJobManager()->SubscribeResourcesUpdated(BIND(
        &TPeriodicInvoker::ScheduleOutOfBand,
        HeartbeatInvoker));

    HeartbeatInvoker->Start();
}

void TSchedulerConnector::SendHeartbeat()
{
    auto jobManager = Bootstrap->GetJobManager();

    // Construct state snapshot.
    auto req = Proxy.Heartbeat();
    ToProto(req->mutable_node_descriptor(), Bootstrap->GetLocalDescriptor());
    *req->mutable_resource_limits() = jobManager->GetResourceLimits();
    *req->mutable_resource_usage() = jobManager->GetResourceUsage();

    auto jobs = Bootstrap->GetJobManager()->GetJobs();
    FOREACH (auto job, jobs) {
        auto state = job->GetState();
        auto* jobStatus = req->add_jobs();
        ToProto(jobStatus->mutable_job_id(), job->GetId());
        jobStatus->set_state(state);
        jobStatus->set_phase(job->GetPhase());
        jobStatus->set_progress(job->GetProgress());
        switch (state) {
            case EJobState::Running:
                *jobStatus->mutable_resource_usage() = job->GetResourceUsage();
                break;

            case EJobState::Completed:
            case EJobState::Aborted:
            case EJobState::Failed: {
                *jobStatus->mutable_result() = job->GetResult();
                break;
            }

            default:
                break;
        }
    }

    req->Invoke().Subscribe(
        BIND(&TSchedulerConnector::OnHeartbeatResponse, MakeStrong(this))
            .Via(ControlInvoker));

    LOG_INFO("Scheduler heartbeat sent (JobCount: %d, ResourceUsage: {%s})",
        req->jobs_size(),
        ~FormatResourceUsage(
            req->resource_usage(),
            req->resource_limits()));
}

void TSchedulerConnector::OnHeartbeatResponse(TSchedulerServiceProxy::TRspHeartbeatPtr rsp)
{
    HeartbeatInvoker->ScheduleNext();

    if (!rsp->IsOK()) {
        LOG_ERROR(*rsp, "Error reporting heartbeat to scheduler");
        return;
    }

    LOG_INFO("Successfully reported heartbeat to scheduler");

    // Handle actions requested by the scheduler.
    auto jobManager = Bootstrap->GetJobManager();

    FOREACH (const auto& protoJobId, rsp->jobs_to_remove()) {
        auto jobId = FromProto<TJobId>(protoJobId);
        RemoveJob(jobId);
    }

    FOREACH (const auto& protoJobId, rsp->jobs_to_abort()) {
        auto jobId = FromProto<TJobId>(protoJobId);
        AbortJob(jobId);
    }

    FOREACH (auto& info, *rsp->mutable_jobs_to_start()) {
        StartJob(info);
    }
}

void TSchedulerConnector::StartJob(TJobStartInfo& info)
{
    auto jobManager = Bootstrap->GetJobManager();
    auto jobId = FromProto<TJobId>(info.job_id());
    const auto& resourceLimits = info.resource_limits();
    auto* spec = info.mutable_spec();
    jobManager->CreateJob(jobId, resourceLimits, *spec);
}

void TSchedulerConnector::AbortJob(const TJobId& jobId)
{
    Bootstrap->GetJobManager()->AbortJob(jobId);
}

void TSchedulerConnector::RemoveJob(const TJobId& jobId)
{
    Bootstrap->GetJobManager()->RemoveJob(jobId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
