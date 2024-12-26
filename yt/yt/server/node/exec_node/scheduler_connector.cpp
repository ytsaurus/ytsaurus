#include "scheduler_connector.h"

#include "allocation.h"
#include "bootstrap.h"
#include "job_controller.h"
#include "master_connector.h"
#include "private.h"
#include "slot_manager.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/scheduler/allocation_tracker_service_proxy.h>

#include <yt/yt/server/lib/misc/job_reporter.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NExecNode {

using namespace NTracing;
using namespace NJobAgent;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient;
using namespace NObjectClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TSchedulerConnector::TSchedulerConnector(IBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , DynamicConfig_(New<TSchedulerConnectorDynamicConfig>())
    , HeartbeatExecutor_(New<TRetryingPeriodicExecutor>(
        Bootstrap_->GetControlInvoker(),
        BIND([this, weakThis = MakeWeak(this)] {
            auto this_ = weakThis.Lock();
            return this_ ? SendHeartbeat() : TError("Scheduler connector is destroyed");
        }),
        DynamicConfig_.Acquire()->HeartbeatExecutor))
    , TimeBetweenSentHeartbeatsCounter_(SchedulerConnectorProfiler().Timer("/time_between_sent_heartbeats"))
    , TimeBetweenAcknowledgedHeartbeatsCounter_(SchedulerConnectorProfiler().Timer("/time_between_acknowledged_heartbeats"))
    , TimeBetweenFullyProcessedHeartbeatsCounter_(SchedulerConnectorProfiler().Timer("/time_between_fully_processed_heartbeats"))
    , PendingResourceHolderHeartbeatSkippedCounter_(
        HeartbeatOutOfBandAttemptsProfiler()
            .WithTag("reason", "pending_resource_holders")
            .Counter("/skipped"))
    , NotEnoughResourcesHeartbeatSkippedCounter_(
        HeartbeatOutOfBandAttemptsProfiler()
            .WithTag("reason", "not_enough_resources")
            .Counter("/skipped"))
    , ResourcesAcquiredHeartbeatRequestedCounter_(
        HeartbeatOutOfBandAttemptsProfiler()
            .WithTag("reason", "resources_acquired")
            .Counter("/requested"))
    , ResourcesReleasedHeartbeatRequestedCounter_(
        HeartbeatOutOfBandAttemptsProfiler()
            .WithTag("reason", "resources_released")
            .Counter("/requested"))
    , AllocationFinishedHeartbeatRequestedCounter_(
        HeartbeatOutOfBandAttemptsProfiler()
            .WithTag("reason", "allocation_finished")
            .Counter("/requested"))
    , TracingSampler_(New<TSampler>(
        DynamicConfig_.Acquire()->TracingSampler,
        SchedulerConnectorProfiler().WithPrefix("/tracing")))
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetControlInvoker(), ControlThread);
}

void TSchedulerConnector::Initialize()
{
    const auto& jobResourceManager = Bootstrap_->GetJobResourceManager();

    jobResourceManager->SubscribeResourcesAcquired(BIND_NO_PROPAGATE(
        &TSchedulerConnector::OnResourcesAcquired,
        MakeWeak(this)));

    jobResourceManager->SubscribeResourcesReleased(BIND_NO_PROPAGATE(
        &TSchedulerConnector::OnResourcesReleased,
        MakeWeak(this)));

    const auto& masterConnector = Bootstrap_->GetMasterConnector();
    masterConnector->SubscribeMasterConnected(BIND_NO_PROPAGATE(
        &TSchedulerConnector::OnMasterConnected,
        MakeWeak(this)));
    masterConnector->SubscribeMasterDisconnected(BIND_NO_PROPAGATE(
        &TSchedulerConnector::OnMasterDisconnected,
        MakeWeak(this)));
}

void TSchedulerConnector::Start()
{ }

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

    TracingSampler_->UpdateConfig(newConfig->TracingSampler);
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
    auto resourceUsage = DynamicConfig_.Acquire()->IncludeReleasingResourcesInSchedulerHeartbeat
        ? jobResourceManager->GetResourceUsage({
            NJobAgent::EResourcesState::Pending,
            NJobAgent::EResourcesState::Acquired,
            NJobAgent::EResourcesState::Releasing,
        })
        : jobResourceManager->GetResourceUsage({
            NJobAgent::EResourcesState::Pending,
            NJobAgent::EResourcesState::Acquired,
        });
    auto freeResources = ToJobResources(ToNodeResources(MakeNonnegative(resourceLimits - resourceUsage)));

    auto pendingResourceHolderCount = jobResourceManager->GetPendingResourceHolderCount();
    if (pendingResourceHolderCount > 0) {
        PendingResourceHolderHeartbeatSkippedCounter_.Increment();
        YT_LOG_DEBUG(
            "Skipping out of band heartbeat because of pending resource holders (PendingResourceHolderCount: %v)",
            pendingResourceHolderCount);

        return;
    }

    if (Dominates(MinSpareResources_, freeResources)) {
        NotEnoughResourcesHeartbeatSkippedCounter_.Increment();
        YT_LOG_DEBUG(
            "Skipping out of band heartbeat because of not enough resources (FreeResources: %v, MinSpareResources: %v)",
            freeResources,
            MinSpareResources_);

        return;
    }

    scheduleOutOfBandHeartbeat();
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

    ResourcesAcquiredHeartbeatRequestedCounter_.Increment();

    SendOutOfBandHeartbeatIfNeeded();
}

void TSchedulerConnector::OnResourcesReleased()
{
    VERIFY_THREAD_AFFINITY_ANY();

    ResourcesReleasedHeartbeatRequestedCounter_.Increment();

    if (DynamicConfig_.Acquire()->SendHeartbeatOnResourcesReleased) {
        SendOutOfBandHeartbeatIfNeeded();
    }
}

void TSchedulerConnector::SetMinSpareResources(const NScheduler::TJobResources& minSpareResources)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetJobInvoker());

    YT_LOG_INFO(
        "Seting new min spare resources (MinSpareResources: %v)",
        minSpareResources);

    MinSpareResources_ = minSpareResources;
}

void TSchedulerConnector::EnqueueFinishedAllocation(TAllocationPtr allocation)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetJobInvoker());

    YT_LOG_DEBUG(
        "Finished allocation enqueued, send out of band heartbeat to scheduler (AllocationId: %v)",
        allocation->GetId());

    FinishedAllocations_.emplace(std::move(allocation));

    AllocationFinishedHeartbeatRequestedCounter_.Increment();

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
    auto nodeId = Bootstrap_->GetNodeId();

    TTraceContextPtr requestTraceContext;
    bool enableTracing = DynamicConfig_.Acquire()->EnableTracing;

    if (enableTracing) {
        requestTraceContext = TTraceContext::NewRoot("SchedulerHeartbeat");
        requestTraceContext->SetRecorded();
        requestTraceContext->AddTag("node_id", nodeId);

        static const TString SchedulerConnectorTracingUserName = "scheduler_connector";
        TracingSampler_->SampleTraceContext(SchedulerConnectorTracingUserName, requestTraceContext);
    }

    auto contextGuard = TTraceContextGuard(requestTraceContext);

    auto req = proxy.Heartbeat();
    req->SetRequestCodec(NCompression::ECodec::Lz4);
    req->SetTimeout(DynamicConfig_.Acquire()->HeartbeatTimeout);

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

    return operationSuccessful
        ? TError()
        : TError("Scheduling skipped");
}

void TSchedulerConnector::OnMasterConnected()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_INFO("Starting heartbeats to scheduler");

    HeartbeatExecutor_->Start();
}

void TSchedulerConnector::OnMasterDisconnected()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_INFO("Stopping heartbeats to scheduler");

    YT_UNUSED_FUTURE(HeartbeatExecutor_->Stop());
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

    const auto& jobResourceManager = Bootstrap_->GetJobResourceManager();

    {
        auto resourceLimits = ToNodeResources(jobResourceManager->GetResourceLimits());
        auto diskResources = jobResourceManager->GetDiskResources();

        bool includeReleasingResources = DynamicConfig_.Acquire()->IncludeReleasingResourcesInSchedulerHeartbeat;
        auto resourceUsage = includeReleasingResources
            ? ToNodeResources(jobResourceManager->GetResourceUsage({
                NJobAgent::EResourcesState::Pending,
                NJobAgent::EResourcesState::Acquired,
                NJobAgent::EResourcesState::Releasing,
            }))
            : ToNodeResources(jobResourceManager->GetResourceUsage({
                NJobAgent::EResourcesState::Pending,
                NJobAgent::EResourcesState::Acquired,
            }));

        YT_LOG_DEBUG(
            "Reporting resource usage to scheduler (IncludeReleasingResources: %v, Usage: %v, Limits: %v, DiskResources: %v)",
            includeReleasingResources,
            resourceUsage,
            resourceLimits,
            diskResources);

        *request->mutable_resource_limits() = resourceLimits;
        *request->mutable_resource_usage() = resourceUsage;
        *request->mutable_disk_resources() = diskResources;
    }

    const auto& jobController = Bootstrap_->GetJobController();
    jobController->PrepareSchedulerHeartbeatRequest(request, context);
}

void TSchedulerConnector::DoProcessHeartbeatResponse(
    const TRspHeartbeatPtr& response,
    const TSchedulerHeartbeatContextPtr& context)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetJobInvoker());

    if (DynamicConfig_.Acquire()->UseProfilingTagsFromScheduler && response->has_profiling_tags()) {
        std::vector<NProfiling::TTag> tags;
        tags.reserve(response->profiling_tags().tags_size());
        for (const auto& tag : response->profiling_tags().tags()) {
            tags.push_back(NProfiling::TTag{
                tag.key(),
                tag.value(),
            });
        }

        Bootstrap_->UpdateNodeProfilingTags(std::move(tags));
    }

    if (response->has_operations_archive_version()) {
        Bootstrap_->GetJobReporter()->SetOperationsArchiveVersion(response->operations_archive_version());
    }

    const auto& jobController = Bootstrap_->GetJobController();
    jobController->ProcessSchedulerHeartbeatResponse(response, context);

    RemoveSentAllocations(context->FinishedAllocations);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
