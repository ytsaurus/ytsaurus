#include "scheduler_connector.h"

#include "allocation.h"
#include "bootstrap.h"
#include "job_controller.h"
#include "master_connector.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/exec_node/slot_manager.h>

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/scheduler/allocation_tracker_service_proxy.h>

#include <yt/yt/server/lib/misc/job_reporter.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>

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

TSchedulerConnector::TSchedulerConnector(IBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , DynamicConfig_(New<TSchedulerConnectorDynamicConfig>())
    , HeartbeatExecutor_(New<TRetryingPeriodicExecutor>(
        Bootstrap_->GetControlInvoker(),
        BIND([weakThis = MakeWeak(this)] {
            auto strongThis = weakThis.Lock();
            return strongThis ? strongThis->SendHeartbeat() : TError("Scheduler connector is destroyed");
        }),
        DynamicConfig_.Acquire()->HeartbeatExecutor))
    , TimeBetweenSentHeartbeatsCounter_(ExecNodeProfiler.Timer("/scheduler_connector/time_between_sent_heartbeats"))
    , TimeBetweenAcknowledgedHeartbeatsCounter_(ExecNodeProfiler.Timer("/scheduler_connector/time_between_acknowledged_heartbeats"))
    , TimeBetweenFullyProcessedHeartbeatsCounter_(ExecNodeProfiler.Timer("/scheduler_connector/time_between_fully_processed_heartbeats"))
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetControlInvoker(), ControlThread);
}

void TSchedulerConnector::Start()
{
    const auto& jobResourceManager = Bootstrap_->GetJobResourceManager();
    jobResourceManager->SubscribeResourcesAcquired(BIND_NO_PROPAGATE(
        &TSchedulerConnector::OnResourcesAcquired,
        MakeWeak(this)));

    HeartbeatExecutor_->Start();
}

void TSchedulerConnector::OnDynamicConfigChanged(
    const TSchedulerConnectorDynamicConfigPtr& /*oldConfig*/,
    const TSchedulerConnectorDynamicConfigPtr& newConfig)
{
    VERIFY_THREAD_AFFINITY_ANY();

    DynamicConfig_.Store(newConfig);

    YT_LOG_DEBUG(
        "Set new scheduler heartbeat options (NewPeriod: %v, NewSplay: %v, NewMinBackoff: %v, NewMaxBackoff: %v, NewBackoffMultiplier: %v)",
        newConfig->HeartbeatExecutor.Period,
        newConfig->HeartbeatExecutor.Splay,
        newConfig->HeartbeatExecutor.MinBackoff,
        newConfig->HeartbeatExecutor.MaxBackoff,
        newConfig->HeartbeatExecutor.BackoffMultiplier);
    HeartbeatExecutor_->SetOptions(newConfig->HeartbeatExecutor);
}

void TSchedulerConnector::DoSendOutOfBandHeartbeatIfNeeded()
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetJobInvoker());

    auto scheduleOutOfBandHeartbeat = [&] {
        YT_LOG_DEBUG("Send out of band heartbeat to scheduler");
        HeartbeatExecutor_->ScheduleOutOfBand();
    };

    const auto& jobResourceManager = Bootstrap_->GetJobResourceManager();
    auto resourceLimits = jobResourceManager->GetResourceLimits();
    auto resourceUsage = jobResourceManager->GetResourceUsage(/*includeWaiting*/ true);
    bool hasWaitingResourceHolders = jobResourceManager->GetWaitingResourceHolderCount();

    auto freeResources = MakeNonnegative(resourceLimits - resourceUsage);

    if (!Dominates(MinSpareResources_, ToJobResources(ToNodeResources(freeResources))) &&
        !hasWaitingResourceHolders)
    {
        scheduleOutOfBandHeartbeat();
    }
}

void TSchedulerConnector::SendOutOfBandHeartbeatIfNeeded()
{
    VERIFY_THREAD_AFFINITY_ANY();

    Bootstrap_->GetJobInvoker()->Invoke(
        BIND(&TSchedulerConnector::DoSendOutOfBandHeartbeatIfNeeded, MakeStrong(this)));
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
    if (resourcesConsumerType == EResourcesConsumerType::SchedulerAllocation && fullyReleased) {
        return;
    }

    if (DynamicConfig_.Acquire()->SendHeartbeatOnJobFinished) {
        SendOutOfBandHeartbeatIfNeeded();
    }
}

void TSchedulerConnector::SetMinSpareResources(const NScheduler::TJobResources& minSpareResources)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetJobInvoker());

    MinSpareResources_ = minSpareResources;
}

void TSchedulerConnector::EnqueueFinishedAllocation(TAllocationPtr allocation)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetJobInvoker());

    YT_LOG_DEBUG(
        "Finished allocation enqueued, send out of band heartbeat to scheduler (AllocationId: %v)",
        allocation->GetId());

    FinishedAllocations_.emplace(std::move(allocation));

    HeartbeatExecutor_->ScheduleOutOfBand();
}

void TSchedulerConnector::RemoveSentAllocations(const THashSet<TAllocationPtr>& allocations)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetJobInvoker());

    for (const auto& allocation : allocations) {
        EraseOrCrash(FinishedAllocations_, allocation);
    }
}

TError TSchedulerConnector::DoSendHeartbeat()
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
        auto [minBackoff, maxBackoff] = HeartbeatExecutor_->GetBackoffInterval();
        YT_LOG_ERROR(
            rspOrError,
            "Error reporting heartbeat to scheduler (BackoffTime: [%v, %v])",
            minBackoff,
            maxBackoff);
        return TError("Failed to report heartbeat to scheduler");
    }

    YT_LOG_INFO("Successfully reported heartbeat to scheduler");

    profileInterval(
        std::max(HeartbeatInfo_.LastFullyProcessedHeartbeatTime, HeartbeatInfo_.LastThrottledHeartbeatTime),
        TimeBetweenAcknowledgedHeartbeatsCounter_);

    const auto& rsp = rspOrError.Value();
    bool operationSuccessful = !rsp->scheduling_skipped();
    if (operationSuccessful) {
        profileInterval(
            HeartbeatInfo_.LastFullyProcessedHeartbeatTime,
            TimeBetweenFullyProcessedHeartbeatsCounter_);
        HeartbeatInfo_.LastFullyProcessedHeartbeatTime = TInstant::Now();
    } else {
        auto [minBackoff, maxBackoff] = HeartbeatExecutor_->GetBackoffInterval();
        YT_LOG_DEBUG(
            "Failed to report heartbeat to scheduler because scheduling was skipped (BackoffTime: [%v, %v])",
            minBackoff,
            maxBackoff);
        HeartbeatInfo_.LastThrottledHeartbeatTime = TInstant::Now();
    }

    ProcessHeartbeatResponse(rsp, heartbeatContext);

    return
        operationSuccessful ?
        TError() :
        TError("Scheduling skipped");
}

TError TSchedulerConnector::SendHeartbeat()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!Bootstrap_->IsConnected() || !Bootstrap_->GetSlotManager()->IsInitialized()) {
        return TError();
    }

    return DoSendHeartbeat();
}

void TSchedulerConnector::PrepareHeartbeatRequest(
    const TReqHeartbeatPtr& request,
    const TSchedulerHeartbeatContextPtr& context)
{
    VERIFY_THREAD_AFFINITY_ANY();

    SetNodeInfoToRequest(
        Bootstrap_->GetNodeId(),
        Bootstrap_->GetLocalDescriptor(),
        request);

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

    context->FinishedAllocations = FinishedAllocations_;

    const auto& jobController = Bootstrap_->GetJobController();
    jobController->PrepareSchedulerHeartbeatRequest(request, context);
}

void TSchedulerConnector::DoProcessHeartbeatResponse(
    const TRspHeartbeatPtr& response,
    const TSchedulerHeartbeatContextPtr& context)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetJobInvoker());

    if (response->has_operations_archive_version()) {
        Bootstrap_->GetJobReporter()->SetOperationsArchiveVersion(response->operations_archive_version());
    }

    const auto& jobController = Bootstrap_->GetJobController();
    jobController->ProcessSchedulerHeartbeatResponse(response, context);

    RemoveSentAllocations(context->FinishedAllocations);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
