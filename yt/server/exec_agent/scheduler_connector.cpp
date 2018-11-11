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

namespace NYT {
namespace NExecAgent {

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
    auto now = TInstant::Now();
    LastSentHeartbeatTime_ = now;
    LastThrottledHeartbeatTime_ = now;
    LastFullyProcessedHeartbeatTime_ = now;
    FailedHeartbeatBackoff_ = Config_->FailedHeartbeatBackoffStartTime;

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
    auto masterConnector = Bootstrap_->GetMasterConnector();
    if (!masterConnector->IsConnected()) {
        return;
    }

    if (TInstant::Now() < std::max(LastFailedHeartbeatTime_, LastThrottledHeartbeatTime_) + FailedHeartbeatBackoff_) {
        LOG_INFO("Skipping heartbeat");
        return;
    }

    const auto& client = Bootstrap_->GetMasterClient();

    TJobTrackerServiceProxy proxy(client->GetSchedulerChannel());
    auto req = proxy.Heartbeat();
    req->SetCodec(NCompression::ECodec::Lz4);

    auto jobController = Bootstrap_->GetJobController();
    auto masterConnection = client->GetNativeConnection();
    jobController->PrepareHeartbeatRequest(
        masterConnection->GetPrimaryMasterCellTag(),
        EObjectType::SchedulerJob,
        req.Get());

    LOG_INFO("Scheduler heartbeat sent (ResourceUsage: %v)",
        FormatResourceUsage(req->resource_usage(), req->resource_limits(), req->disk_info()));

    auto timeBetweenSentHeartbeats = TInstant::Now() - LastSentHeartbeatTime_;
    Profiler.Update(
        TimeBetweenSentHeartbeatsCounter_,
        timeBetweenSentHeartbeats.MilliSeconds());

    LastSentHeartbeatTime_ = TInstant::Now();

    auto rspOrError = WaitFor(req->Invoke());

    if (!rspOrError.IsOK()) {
        LastFailedHeartbeatTime_ = TInstant::Now();
        FailedHeartbeatBackoff_ = std::min(
            FailedHeartbeatBackoff_ * Config_->FailedHeartbeatBackoffMultiplier,
            Config_->FailedHeartbeatBackoffMaxTime);
        LOG_ERROR(rspOrError, "Error reporting heartbeat to scheduler");
        return;
    }

    LOG_INFO("Successfully reported heartbeat to scheduler");
г
    const auto& rsp = rspOrError.Value();

    auto now = TInstant::Now();
    auto timeBetweenAcknowledgedHeartbeats = now - std::max(LastFullyProcessedHeartbeatTime_, LastThrottledHeartbeatTime_);
    Profiler.Update(
        TimeBetweenAcknowledgedHeartbeatsCounter_,
        timeBetweenAcknowledgedHeartbeats.MilliSeconds());

    FailedHeartbeatBackoff_ = Config_->FailedHeartbeatBackoffStartTime;

    if (rsp->scheduling_skipped()) {
        LastThrottledHeartbeatTime_ = now;
    } else {
        auto timeBetweenFullyProcessedHeartbeats = now - LastFullyProcessedHeartbeatTime_;
        Profiler.Update(
            TimeBetweenFullyProcessedHeartbeatsCounter_,
            timeBetweenFullyProcessedHeartbeats.MilliSeconds());
        LastFullyProcessedHeartbeatTime_ = now;
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

} // namespace NExecAgent
} // namespace NYT
