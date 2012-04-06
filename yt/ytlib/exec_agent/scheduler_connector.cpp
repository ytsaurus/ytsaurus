#include "stdafx.h"
#include "scheduler_connector.h"
#include "private.h"
#include "bootstrap.h"
#include "job_manager.h"
#include "job.h"

#include <util/random/random.h>

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
    , Proxy(bootstrap->GetSchedulerChannel())
    , HeartbeatInProgress(false)
    , OutOfOrderHeartbeatNeeded(false)
{
    YASSERT(config);
    YASSERT(bootstrap);
}

void TSchedulerConnector::Start()
{
    // Send the first heartbeat with a random delay.
    TDelayedInvoker::Submit(
        BIND(&TSchedulerConnector::SendHeartbeat, MakeStrong(this))
        .Via(Bootstrap->GetControlInvoker()),
        RandomNumber(Config->HeartbeatSplay));
}

void TSchedulerConnector::ScheduleNextHeartbeat()
{
    YASSERT(!HeartbeatInProgress);

    auto callback =
        BIND(&TSchedulerConnector::SendHeartbeat, MakeStrong(this))
        .Via(Bootstrap->GetControlInvoker());

    if (OutOfOrderHeartbeatNeeded) {
        OutOfOrderHeartbeatNeeded = false;
        callback.Run();
    } else {
        HeartbeatCookie = TDelayedInvoker::Submit(callback, Config->HeartbeatPeriod);
    }
}

void TSchedulerConnector::SendHeartbeat()
{
    YASSERT(!HeartbeatInProgress);
    HeartbeatInProgress = true;
    TDelayedInvoker::CancelAndClear(HeartbeatCookie);

    // Construct state snapshot.
    auto req = Proxy.Heartbeat();
    req->set_address(Bootstrap->GetPeerAddress());
    auto jobManager = Bootstrap->GetJobManager();
    *req->mutable_utilization() = jobManager->GetUtilization();

    auto jobs = Bootstrap->GetJobManager()->GetAllJobs();
    FOREACH (auto job, jobs) {
        auto state = job->GetState();
        auto* jobStatus = req->add_jobs();
        *jobStatus->mutable_job_id() = job->GetId().ToProto();
        jobStatus->set_state(state);
        jobStatus->set_progress(job->GetProgress());
        if (state == EJobState::Completed || state == EJobState::Failed) {
            *jobStatus->mutable_result() = job->GetResult();
        }
    }

    req->Invoke()->Subscribe(
        BIND(&TSchedulerConnector::OnHeartbeatResponse, MakeStrong(this))
        .Via(Bootstrap->GetControlInvoker()));

    LOG_INFO("Scheduler heartbeat sent (JobCount: %d, TotalSlotCount: %d, FreeSlotCount: %d)",
        req->jobs_size(),
        req->utilization().total_slot_count(),
        req->utilization().free_slot_count());
}

void TSchedulerConnector::OnHeartbeatResponse(TSchedulerServiceProxy::TRspHeartbeat::TPtr rsp)
{
    YASSERT(HeartbeatInProgress);
    HeartbeatInProgress = false;

    ScheduleNextHeartbeat();

    if (!rsp->IsOK()) {
        LOG_ERROR("Error reporting heartbeat to scheduler\n%s", ~rsp->GetError().ToString());
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

    FOREACH (const auto& info, rsp->jobs_to_start()) {
        StartJob(info);
    }
}

void TSchedulerConnector::StartJob(const TStartJobInfo& info)
{
    auto jobId = TJobId::FromProto(info.job_id());
    const auto& spec = info.spec();
    auto job = Bootstrap->GetJobManager()->StartJob(jobId, spec);
    // Schedule an out-of-order heartbeat whenever a job finishes.
    job->SubscribeFinished(
        BIND([=] () {
            if (HeartbeatInProgress) {
                OutOfOrderHeartbeatNeeded = true;
            } else {
                SendHeartbeat();
            }
        })
        .Via(Bootstrap->GetControlInvoker()));
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
