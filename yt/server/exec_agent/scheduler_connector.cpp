#include "scheduler_connector.h"
#include "private.h"
#include "config.h"
#include "job.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/server/data_node/master_connector.h>

#include <yt/server/job_agent/job_controller.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/core/concurrency/scheduler.h>

namespace NYT::NExecAgent {

using namespace NNodeTrackerClient;
using namespace NJobTrackerClient;
using namespace NObjectClient;
using namespace NCellNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecAgentLogger;
static const auto& Profiler = ExecAgentProfiler;

////////////////////////////////////////////////////////////////////////////////

TSchedulerConnector::TSchedulerConnector(
    TSchedulerConnectorConfigPtr config,
    TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
    , ControlInvoker_(bootstrap->GetControlInvoker())
    , TimeBetweenSentHeartbeatsCounter_("/scheduler_connector/time_between_send_heartbeats")
    , TimeBetweenAcknowledgedHeartbeatsCounter_("/scheduler_connector/time_between_acknowledged_heartbeats")
    , TimeBetweenFullyProcessedHeartbeatsCounter_("/scheduler_connector/time_between_fully_processed_heartbeats")
{
    YCHECK(config);
    YCHECK(bootstrap);
}

void TSchedulerConnector::Start()
{
    HeartbeatExecutor_ = New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TSchedulerConnector::SendHeartbeat, MakeWeak(this)),
        Config_->HeartbeatPeriod,
        EPeriodicExecutorMode::Automatic,
        Config_->HeartbeatSplay);

    // Schedule an out-of-order heartbeat whenever a job finishes
    // or its resource usage is updated.
    auto jobController = Bootstrap_->GetJobController();
    jobController->SubscribeResourcesUpdated(BIND(
        &TPeriodicExecutor::ScheduleOutOfBand,
        HeartbeatExecutor_));

    HeartbeatExecutor_->Start();
}

void TSchedulerConnector::SendHeartbeat()
{
    const auto& masterConnector = Bootstrap_->GetMasterConnector();
    if (!masterConnector->IsConnected()) {
        return;
    }

    if (TInstant::Now() < std::max(LastFailedHeartbeatTime_, LastThrottledHeartbeatTime_) + FailedHeartbeatBackoffTime_) {
        LOG_INFO("Skipping heartbeat");
        return;
    }

    const auto& client = Bootstrap_->GetMasterClient();

    TJobTrackerServiceProxy proxy(client->GetSchedulerChannel());
    auto req = proxy.Heartbeat();
    req->SetCodec(NCompression::ECodec::Lz4);

    const auto& jobController = Bootstrap_->GetJobController();
    const auto masterConnection = client->GetNativeConnection();
    jobController->PrepareHeartbeatRequest(
        masterConnection->GetPrimaryMasterCellTag(),
        EObjectType::SchedulerJob,
        req.Get());

    LOG_INFO("Scheduler heartbeat sent (ResourceUsage: %v)",
        FormatResourceUsage(req->resource_usage(), req->resource_limits(), req->disk_info()));

    auto profileInterval = [&] (TInstant lastTime, NProfiling::TAggregateGauge& counter) {
        if (lastTime != TInstant::Zero()) {
            auto delta = TInstant::Now() - lastTime;
            Profiler.Update(counter, NProfiling::DurationToValue(delta));
        }
    };

    profileInterval(LastSentHeartbeatTime_, TimeBetweenSentHeartbeatsCounter_);
    LastSentHeartbeatTime_ = TInstant::Now();

    auto rspOrError = WaitFor(req->Invoke());

    if (!rspOrError.IsOK()) {
        LastFailedHeartbeatTime_ = TInstant::Now();
        if (FailedHeartbeatBackoffTime_ == TDuration::Zero()) {
            FailedHeartbeatBackoffTime_ = Config_->FailedHeartbeatBackoffStartTime;
        } else {
            FailedHeartbeatBackoffTime_ = std::min(
                FailedHeartbeatBackoffTime_ * Config_->FailedHeartbeatBackoffMultiplier,
                Config_->FailedHeartbeatBackoffMaxTime);
        }
        LOG_ERROR(rspOrError, "Error reporting heartbeat to scheduler (BackoffTime: %v)",
            FailedHeartbeatBackoffTime_);
        return;
    }

    LOG_INFO("Successfully reported heartbeat to scheduler");

    FailedHeartbeatBackoffTime_ = TDuration::Zero();

    profileInterval(std::max(LastFullyProcessedHeartbeatTime_, LastThrottledHeartbeatTime_), TimeBetweenAcknowledgedHeartbeatsCounter_);

    const auto& rsp = rspOrError.Value();
    if (rsp->scheduling_skipped()) {
        LastThrottledHeartbeatTime_ = TInstant::Now();
    } else {
        profileInterval(LastFullyProcessedHeartbeatTime_, TimeBetweenFullyProcessedHeartbeatsCounter_);
        LastFullyProcessedHeartbeatTime_ = TInstant::Now();
    }

    const auto& reporter = Bootstrap_->GetStatisticsReporter();
    if (rsp->has_enable_job_reporter()) {
        reporter->SetEnabled(rsp->enable_job_reporter());
    }
    if (rsp->has_enable_job_spec_reporter()) {
        reporter->SetSpecEnabled(rsp->enable_job_spec_reporter());
    }
    if (rsp->has_enable_job_stderr_reporter()) {
        reporter->SetStderrEnabled(rsp->enable_job_stderr_reporter());
    }
    if (rsp->has_enable_job_fail_context_reporter()) {
        reporter->SetFailContextEnabled(rsp->enable_job_fail_context_reporter());
    }
    if (rsp->has_operation_archive_version()) {
        reporter->SetOperationArchiveVersion(rsp->operation_archive_version());
    }

    jobController->ProcessHeartbeatResponse(rsp, EObjectType::SchedulerJob);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
