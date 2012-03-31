#include "stdafx.h"
#include "scheduler_connector.h"
#include "private.h"
#include "bootstrap.h"
#include "job_manager.h"
#include "job.h"


namespace NYT {
namespace NExecAgent {

using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TSchedulerConnector::TSchedulerConnector(
    TSchedulerConnectorConfigPtr config,
    TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
    , Proxy(bootstrap->GetSchedulerChannel())
{
    YASSERT(config);
    YASSERT(bootstrap);
}

void TSchedulerConnector::Start()
{
    PeriodicInvoker = New<TPeriodicInvoker>(
        BIND(&TSchedulerConnector::SendHeartbeat, MakeWeak(this))
        .Via(Bootstrap->GetControlInvoker()),
        Config->HeartbeatPeriod);
    PeriodicInvoker->Start();
}

void TSchedulerConnector::SendHeartbeat()
{
    // Construct state snapshot.
    auto req = Proxy.Heartbeat();
    req->set_address(Bootstrap->GetPeerAddress());
    auto jobManager = Bootstrap->GetJobManager();
    *req->mutable_utilization() = jobManager->GetUtilization();

    auto jobs = Bootstrap->GetJobManager()->GetAllJobs();
    FOREACH (auto job, jobs) {
        auto state = job->GetState();
        auto* jobStatus = req->add_jobs();
        jobStatus->set_job_id(job->GetId().ToProto());
        jobStatus->set_state(state);
        jobStatus->set_progress(job->GetProgress());
        if (state == EJobState::Completed || state == EJobState::Failed) {
            *jobStatus->mutable_result() = job->GetResult();
        }
    }

    LOG_INFO("Sending heartbeat to scheduler (JobCount: %d, TotalSlotCount: %d, FreeSlotCount: %d)",
        req->jobs_size(),
        req->utilization().total_slot_count(),
        req->utilization().free_slot_count());

    req->Invoke()->Subscribe(
        BIND(&TSchedulerConnector::OnHeartbeatResponse, MakeStrong(this))
        .Via(Bootstrap->GetControlInvoker()));
}

void TSchedulerConnector::OnHeartbeatResponse(TSchedulerServiceProxy::TRspHeartbeat::TPtr rsp)
{
    if (!rsp->IsOK()) {
        LOG_ERROR("Error reporting heartbeat to scheduler\n%s", ~rsp->GetError().ToString());
        return;
    }

    LOG_INFO("Successfully reported heartbeat to scheduler");

    // Handle actions.
    auto jobManager = Bootstrap->GetJobManager();

    FOREACH (const auto& protoJobId, rsp->jobs_to_remove()) {
        auto jobId = TJobId::FromProto(protoJobId);
        jobManager->RemoveJob(jobId);
    }

    FOREACH (const auto& protoJobId, rsp->jobs_to_abort()) {
        auto jobId = TJobId::FromProto(protoJobId);
        // TODO(babenko): rename to AbortJob
        jobManager->AbortJob(jobId);
    }

    FOREACH (const auto& startInfo, rsp->jobs_to_start()) {
        auto jobId = TJobId::FromProto(startInfo.job_id());
        const auto& spec = startInfo.spec();
        jobManager->StartJob(jobId, spec);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
