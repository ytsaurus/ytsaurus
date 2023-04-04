#include "scheduler_connector.h"

#include "bootstrap.h"
#include "job.h"
#include "job_controller.h"
#include "master_connector.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/exec_node/slot_manager.h>

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/job_agent/job_reporter.h>

#include <yt/yt/server/lib/scheduler/allocation_tracker_service_proxy.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NExecNode {

using namespace NJobAgent;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient;
using namespace NObjectClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TSchedulerConnector::TSchedulerConnector(
    TSchedulerConnectorConfigPtr config,
    IBootstrap* bootstrap)
    : StaticConfig_(config)
    , CurrentConfig_(CloneYsonStruct(StaticConfig_))
    , Bootstrap_(bootstrap)
    , HeartbeatExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetControlInvoker(),
        BIND(
            &TSchedulerConnector::SendHeartbeat,
            MakeWeak(this)),
            TPeriodicExecutorOptions{
                .Period = StaticConfig_->HeartbeatPeriod,
                .Splay = StaticConfig_->HeartbeatSplay
            }))
    , TimeBetweenSentHeartbeatsCounter_(ExecNodeProfiler.Timer("/scheduler_connector/time_between_sent_heartbeats"))
    , TimeBetweenAcknowledgedHeartbeatsCounter_(ExecNodeProfiler.Timer("/scheduler_connector/time_between_acknowledged_heartbeats"))
    , TimeBetweenFullyProcessedHeartbeatsCounter_(ExecNodeProfiler.Timer("/scheduler_connector/time_between_fully_processed_heartbeats"))
{
    YT_VERIFY(config);
    YT_VERIFY(bootstrap);
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetControlInvoker(), ControlThread);
}

void TSchedulerConnector::DoSendOutOfBandHeartbeatIfNeeded()
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetJobInvoker());

    auto scheduleOutOfBandHeartbeat = [&] {
        HeartbeatExecutor_->ScheduleOutOfBand();
    };

    const auto& jobResourceManager = Bootstrap_->GetJobResourceManager();
    auto resourceLimits = jobResourceManager->GetResourceLimits();
    auto resourceUsage = jobResourceManager->GetResourceUsage(/*includeWaiting*/ true);
    bool hasWaitingResourceHolders = jobResourceManager->GetWaitingResourceHolderCount();

    auto freeResources = MakeNonnegative(resourceLimits - resourceUsage);

    if (!Dominates(MinSpareResources_, ToJobResources(freeResources)) && !hasWaitingResourceHolders) {
        scheduleOutOfBandHeartbeat();
    }
}

void TSchedulerConnector::SendOutOfBandHeartbeatIfNeeded()
{
    VERIFY_THREAD_AFFINITY_ANY();

    Bootstrap_->GetJobInvoker()->Invoke(
        BIND(&TSchedulerConnector::DoSendOutOfBandHeartbeatIfNeeded, MakeStrong(this)));
}

void TSchedulerConnector::OnJobFinished(const TJobPtr& job)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetJobInvoker());

    EnqueueFinishedJobs({job});

    HeartbeatExecutor_->ScheduleOutOfBand();
}

void TSchedulerConnector::OnResourcesAcquired()
{
    VERIFY_THREAD_AFFINITY_ANY();

    SendOutOfBandHeartbeatIfNeeded();
}

void TSchedulerConnector::OnResourcesReleased(
    EResourcesConsumerType resourcesConsumerType,
    bool fullyReleased)
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Scheduler connector is subscribed to JobFinished scheduler job controller signal.
    if (resourcesConsumerType == EResourcesConsumerType::SchedulerJob && fullyReleased) {
        return;
    }

    if (SendHeartbeatOnJobFinished_.load(std::memory_order::relaxed)) {
        SendOutOfBandHeartbeatIfNeeded();
    }
}

void TSchedulerConnector::SetMinSpareResources(const NScheduler::TJobResources& minSpareResources)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetJobInvoker());

    MinSpareResources_ = minSpareResources;
}

void TSchedulerConnector::EnqueueFinishedJobs(std::vector<TJobPtr> jobs)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetJobInvoker());

    for (auto& job : jobs) {
        JobsToForcefullySend_.emplace(std::move(job));
    }
}

void TSchedulerConnector::AddUnconfirmedJobs(const std::vector<TJobId>& unconfirmedJobIds)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetJobInvoker());

    for (auto jobId : unconfirmedJobIds) {
        UnconfirmedJobIds_.emplace(jobId);
    }
}

void TSchedulerConnector::EnqueueSpecFetchFailedAllocation(TAllocationId allocationId, TSpecFetchFailedAllocationInfo info)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetJobInvoker());

    EmplaceOrCrash(SpecFetchFailedAllocations_, allocationId, std::move(info));
}

void TSchedulerConnector::RemoveSpecFetchFailedAllocations(THashMap<TAllocationId, TSpecFetchFailedAllocationInfo> allocations)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetJobInvoker());

    for (auto [allocationId, _] : allocations) {
        EraseOrCrash(SpecFetchFailedAllocations_, allocationId);
    }
}

void TSchedulerConnector::Start()
{
    const auto& jobResourceManager = Bootstrap_->GetJobResourceManager();
    jobResourceManager->SubscribeResourcesAcquired(BIND_NO_PROPAGATE(
        &TSchedulerConnector::OnResourcesAcquired,
        MakeWeak(this)));

    const auto& jobController = Bootstrap_->GetJobController();
    jobController->SubscribeJobFinished(BIND_NO_PROPAGATE(
            &TSchedulerConnector::OnJobFinished,
            MakeWeak(this))
        .Via(Bootstrap_->GetJobInvoker()));

    HeartbeatExecutor_->Start();
}

void TSchedulerConnector::OnDynamicConfigChanged(
    const TExecNodeDynamicConfigPtr& oldConfig,
    const TExecNodeDynamicConfigPtr& newConfig)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!newConfig->SchedulerConnector && !oldConfig->SchedulerConnector) {
        return;
    }

    Bootstrap_->GetControlInvoker()->Invoke(BIND([this, this_{MakeStrong(this)}, newConfig{std::move(newConfig)}] {
        if (newConfig->SchedulerConnector) {
            CurrentConfig_ = StaticConfig_->ApplyDynamic(newConfig->SchedulerConnector);
            SendHeartbeatOnJobFinished_.store(newConfig->SchedulerConnector->SendHeartbeatOnJobFinished, std::memory_order::relaxed);
        } else {
            CurrentConfig_ = StaticConfig_;
            SendHeartbeatOnJobFinished_.store(true, std::memory_order::relaxed);
        }
        HeartbeatExecutor_->SetPeriod(CurrentConfig_->HeartbeatPeriod);
    }));
}

void TSchedulerConnector::DoSendHeartbeat()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    const auto& client = Bootstrap_->GetClient();

    TAllocationTrackerServiceProxy proxy(client->GetSchedulerChannel());

    auto req = proxy.Heartbeat();
    req->SetRequestCodec(NCompression::ECodec::Lz4);

    auto heartbeatContext = New<TSchedulerHeartbeatContext>();
    PrepareHeartbeatRequest(req, heartbeatContext);

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

    ProcessHeartbeatResponse(rsp, heartbeatContext);
}

void TSchedulerConnector::SendHeartbeat()
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

    DoSendHeartbeat();
}

void TSchedulerConnector::PrepareHeartbeatRequest(
    const TReqHeartbeatPtr& request,
    const TSchedulerHeartbeatContextPtr& context)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto error = WaitFor(BIND(
            &TSchedulerConnector::DoPrepareHeartbeatRequest,
            MakeStrong(this),
            request,
            context)
        .AsyncVia(Bootstrap_->GetJobInvoker())
        .Run());

    YT_LOG_FATAL_IF(
        !error.IsOK(),
        error,
        "Failed to prepare scheduler heartbeat request");
}

void TSchedulerConnector::ProcessHeartbeatResponse(
    const TRspHeartbeatPtr& response,
    const TSchedulerHeartbeatContextPtr& context)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto error = WaitFor(BIND(
            &TSchedulerConnector::DoProcessHeartbeatResponse,
            MakeStrong(this),
            response,
            context)
        .AsyncVia(Bootstrap_->GetJobInvoker())
        .Run());

    YT_LOG_FATAL_IF(
        !error.IsOK(),
        error,
        "Error while processing scheduler heartbeat response");
}

void TSchedulerConnector::DoPrepareHeartbeatRequest(
    const TReqHeartbeatPtr& request,
    const TSchedulerHeartbeatContextPtr& context)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetJobInvoker());

    context->JobsToForcefullySend = std::move(JobsToForcefullySend_);
    context->UnconfirmedJobIds = std::move(UnconfirmedJobIds_);
    context->SpecFetchFailedAllocations = SpecFetchFailedAllocations_;

    const auto& jobController = Bootstrap_->GetJobController();
    jobController->PrepareSchedulerHeartbeatRequest(request, context);
}

void TSchedulerConnector::DoProcessHeartbeatResponse(
    const TRspHeartbeatPtr& response,
    const TSchedulerHeartbeatContextPtr& context)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetJobInvoker());

    if (response->has_operation_archive_version()) {
        Bootstrap_->GetJobReporter()->SetOperationArchiveVersion(response->operation_archive_version());
    }

    const auto& jobController = Bootstrap_->GetJobController();
    jobController->ProcessSchedulerHeartbeatResponse(response, context);

    RemoveSpecFetchFailedAllocations(std::move(context->SpecFetchFailedAllocations));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
