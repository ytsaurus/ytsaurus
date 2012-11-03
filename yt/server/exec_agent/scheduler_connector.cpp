#include "stdafx.h"
#include "scheduler_connector.h"
#include "private.h"
#include "bootstrap.h"
#include "job_manager.h"
#include "job.h"

#include <server/scheduler/job_resources.h>

namespace NYT {
namespace NExecAgent {

using namespace NScheduler;
using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ExecAgentLogger;

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
        Config->HeartbeatSplay);
    HeartbeatInvoker->Start();
}

void TSchedulerConnector::SendHeartbeat()
{
    auto jobManager = Bootstrap->GetJobManager();

    // Construct state snapshot.
    auto req = Proxy.Heartbeat();
    req->set_address(Bootstrap->GetPeerAddress());
    *req->mutable_resource_limits() = jobManager->GetResourceLimits();
    *req->mutable_resource_utilization() = jobManager->GetResourceUtilization();

    auto jobs = Bootstrap->GetJobManager()->GetJobs();
    FOREACH (auto job, jobs) {
        auto state = job->GetState();
        auto* jobStatus = req->add_jobs();
        *jobStatus->mutable_job_id() = job->GetId().ToProto();
        jobStatus->set_state(state);
        jobStatus->set_phase(job->GetPhase());
        jobStatus->set_progress(job->GetProgress());
        switch (state) {
            case EJobState::Running:
                *jobStatus->mutable_resource_utilization() = job->GetResourceUtilization();
                break;

            case EJobState::Completed:
            case EJobState::Failed: {
                auto& jobResult = job->GetResult();
                *jobStatus->mutable_result() = jobResult;
                break;
            }

            default:
                break;
        }
    }

    req->Invoke().Subscribe(
        BIND(&TSchedulerConnector::OnHeartbeatResponse, MakeStrong(this))
        .Via(ControlInvoker));

    LOG_INFO("Scheduler heartbeat sent (JobCount: %d, Utilization: {%s})",
        req->jobs_size(),
        ~FormatResourceUtilization(
            req->resource_utilization(),
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
        auto jobId = TJobId::FromProto(protoJobId);
        RemoveJob(jobId);
    }

    FOREACH (const auto& protoJobId, rsp->jobs_to_abort()) {
        auto jobId = TJobId::FromProto(protoJobId);
        AbortJob(jobId);
    }

    FOREACH (auto& info, *rsp->mutable_jobs_to_start()) {
        StartJob(info);
    }
}

void TSchedulerConnector::StartJob(TJobStartInfo& info)
{
    auto jobId = TJobId::FromProto(info.job_id());
    auto* spec = info.mutable_spec();
    auto job = Bootstrap->GetJobManager()->StartJob(jobId, *spec);

    // Schedule an out-of-order heartbeat whenever a job finishes
    // or its resource utilization is updated.
    job->SubscribeFinished(BIND(
        &TPeriodicInvoker::ScheduleOutOfBand,
        HeartbeatInvoker));
    job->SubscribeResourceUtilizationSet(BIND(
        &TPeriodicInvoker::ScheduleOutOfBand,
        HeartbeatInvoker));
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
