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
        FromMethod(&TSchedulerConnector::SendHeartbeat, MakeWeak(this))
        ->Via(Bootstrap->GetControlInvoker()),
        Config->HeartbeatPeriod);
    PeriodicInvoker->Start();
}

void TSchedulerConnector::SendHeartbeat()
{
    // Construct state snapshot.
    auto req = Proxy.Heartbeat();
    req->set_address(Bootstrap->GetPeerAddress());
    auto jobManager = Bootstrap->GetJobManager();
    req->set_total_slot_count(jobManager->GetTotalSlotCount());
    req->set_free_slot_count(jobManager->GetFreeSlotCount());

    auto jobs = Bootstrap->GetJobManager()->GetAllJobs();
    FOREACH (auto job, jobs) {
        auto* jobStatus = req->add_jobs();
        jobStatus->set_job_id(job->GetId().ToProto());
        jobStatus->set_state(job->GetState());
        if (job->GetState() == EJobState::Completed) {
            *jobStatus->mutable_result() = job->GetResult();
        }
    }

    LOG_INFO("Sending heartbeat to scheduler (JobCount: %d, TotalSlotCount: %d, FreeSlotCount: %d)",
        req->jobs_size(),
        req->total_slot_count(),
        req->free_slot_count());

    req->Invoke()->Subscribe(
        FromMethod(&TSchedulerConnector::OnHeartbeatResponse, MakeStrong(this))
        ->Via(Bootstrap->GetControlInvoker()));
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

    FOREACH (const auto& protoJobId, rsp->jobs_to_stop()) {
        auto jobId = TJobId::FromProto(protoJobId);
        jobManager->StopJob(jobId);
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
