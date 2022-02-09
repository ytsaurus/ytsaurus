#include "scheduler_connector.h"

#include "bootstrap.h"
#include "private.h"
#include "job.h"
#include "master_connector.h"

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/job_agent/job_reporter.h>

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/exec_node/slot_manager.h>

#include <yt/yt/server/node/job_agent/job_controller.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NExecNode {

using namespace NNodeTrackerClient;
using namespace NJobTrackerClient;
using namespace NObjectClient;
using namespace NClusterNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TSchedulerConnector::TSchedulerConnector(
    TSchedulerConnectorConfigPtr config,
    IBootstrap* bootstrap)
    : StaticConfig_(config)
    , CurrentConfig_(CloneYsonSerializable(StaticConfig_))
    , Bootstrap_(bootstrap)
    , HeartbeatExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetControlInvoker(),
        BIND(&TSchedulerConnector::SendHeartbeat, MakeWeak(this)),
        StaticConfig_->HeartbeatPeriod,
        StaticConfig_->HeartbeatSplay))
    , TimeBetweenSentHeartbeatsCounter_(ExecNodeProfiler.Timer("/scheduler_connector/time_between_sent_heartbeats"))
    , TimeBetweenAcknowledgedHeartbeatsCounter_(ExecNodeProfiler.Timer("/scheduler_connector/time_between_acknowledged_heartbeats"))
    , TimeBetweenFullyProcessedHeartbeatsCounter_(ExecNodeProfiler.Timer("/scheduler_connector/time_between_fully_processed_heartbeats"))
{
    YT_VERIFY(config);
    YT_VERIFY(bootstrap);
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetControlInvoker(), ControlThread);
}

void TSchedulerConnector::Start()
{
    // Schedule an out-of-order heartbeat whenever a job finishes
    // or its resource usage is updated.
    const auto& jobController = Bootstrap_->GetJobController();
    jobController->SubscribeResourcesUpdated(BIND(
        &TPeriodicExecutor::ScheduleOutOfBand,
        HeartbeatExecutor_));

    HeartbeatExecutor_->Start();
}

void TSchedulerConnector::OnDynamicConfigChanged(
    const TExecNodeDynamicConfigPtr& oldConfig,
    const TExecNodeDynamicConfigPtr& newConfig)
{
    if (!newConfig->SchedulerConnector && !oldConfig->SchedulerConnector) {
        return;
    }

    Bootstrap_->GetControlInvoker()->Invoke(BIND([this, this_{MakeStrong(this)}, newConfig{std::move(newConfig)}] {
        if (newConfig->SchedulerConnector) {
            CurrentConfig_ = StaticConfig_->ApplyDynamic(newConfig->SchedulerConnector);
        } else {
            CurrentConfig_ = StaticConfig_;
        }
        HeartbeatExecutor_->SetPeriod(CurrentConfig_->HeartbeatPeriod);
    }));
}

void TSchedulerConnector::SendHeartbeat() noexcept
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!Bootstrap_->IsConnected()) {
        return;
    }

    const auto slotManager = Bootstrap_->GetSlotManager();
    if (!slotManager->IsInitialized()) {
        return;
    }

    if (TInstant::Now() < std::max(
        HeartbeatInfo_.LastFailedHeartbeatTime,
        HeartbeatInfo_.LastThrottledHeartbeatTime) + HeartbeatInfo_.FailedHeartbeatBackoffTime)
    {
        YT_LOG_INFO("Skipping scheduler heartbeat");
        return;
    }

    const auto& client = Bootstrap_->GetMasterClient();

    TJobTrackerServiceProxy proxy(client->GetSchedulerChannel());
    auto req = proxy.Heartbeat();
    req->SetRequestCodec(NCompression::ECodec::Lz4);

    const auto& jobController = Bootstrap_->GetJobController();
    const auto& masterConnection = client->GetNativeConnection();
    YT_VERIFY(WaitFor(jobController->PrepareHeartbeatRequest(
        masterConnection->GetPrimaryMasterCellTag(),
        EObjectType::SchedulerJob,
        req))
        .IsOK());

    auto profileInterval = [&] (TInstant lastTime, NProfiling::TEventTimer& counter) {
        if (lastTime != TInstant::Zero()) {
            auto delta = TInstant::Now() - lastTime;
            counter.Record(delta);
        }
    };

    profileInterval(HeartbeatInfo_.LastSentHeartbeatTime, TimeBetweenSentHeartbeatsCounter_);
    HeartbeatInfo_.LastSentHeartbeatTime = TInstant::Now();

    YT_LOG_INFO("Scheduler heartbeat sent (ResourceUsage: %v)",
        FormatResourceUsage(req->resource_usage(), req->resource_limits(), req->disk_resources()));

    auto rspOrError = WaitFor(req->Invoke());
    if (!rspOrError.IsOK()) {
        HeartbeatInfo_.LastFailedHeartbeatTime = TInstant::Now();
        if (HeartbeatInfo_.FailedHeartbeatBackoffTime == TDuration::Zero()) {
            HeartbeatInfo_.FailedHeartbeatBackoffTime = CurrentConfig_->FailedHeartbeatBackoffStartTime;
        } else {
            HeartbeatInfo_.FailedHeartbeatBackoffTime = std::min(
                HeartbeatInfo_.FailedHeartbeatBackoffTime * CurrentConfig_->FailedHeartbeatBackoffMultiplier,
                CurrentConfig_->FailedHeartbeatBackoffMaxTime);
        }
        YT_LOG_ERROR(rspOrError, "Error reporting heartbeat to scheduler (BackoffTime: %v)",
            HeartbeatInfo_.FailedHeartbeatBackoffTime);
        return;
    }

    YT_LOG_INFO("Successfully reported heartbeat to scheduler");

    HeartbeatInfo_.FailedHeartbeatBackoffTime = TDuration::Zero();

    profileInterval(
        std::max(HeartbeatInfo_.LastFullyProcessedHeartbeatTime, HeartbeatInfo_.LastThrottledHeartbeatTime),
        TimeBetweenAcknowledgedHeartbeatsCounter_);

    const auto& rsp = rspOrError.Value();
    if (rsp->scheduling_skipped()) {
        HeartbeatInfo_.LastThrottledHeartbeatTime = TInstant::Now();
    } else {
        profileInterval(
            HeartbeatInfo_.LastFullyProcessedHeartbeatTime,
            TimeBetweenFullyProcessedHeartbeatsCounter_);
        HeartbeatInfo_.LastFullyProcessedHeartbeatTime = TInstant::Now();
    }

    if (rsp->has_operation_archive_version()) {
        Bootstrap_->GetJobReporter()->SetOperationArchiveVersion(rsp->operation_archive_version());
    }

    const auto result = WaitFor(jobController->ProcessHeartbeatResponse(rsp, EObjectType::SchedulerJob));
    YT_LOG_FATAL_IF(!result.IsOK(), result, "Error while processing scheduler heartbeat response");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
