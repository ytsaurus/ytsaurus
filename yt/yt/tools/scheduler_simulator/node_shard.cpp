#include "node_shard.h"

#include "event_log.h"
#include "operation_controller.h"

#include <yt/yt/ytlib/chunk_client/medium_directory.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NSchedulerSimulator {

using namespace NScheduler;
using namespace NLogging;
using namespace NControllerAgent;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

TJobId JobIdFromAllocationId(TAllocationId allocationId)
{
    return TJobId(allocationId.Underlying());
}

std::unique_ptr<TCompletedJobSummary> BuildCompletedJobSummary(const TAllocationPtr& allocation)
{
    TCompletedJobSummary jobSummary;
    jobSummary.Id = JobIdFromAllocationId(allocation->GetId());
    jobSummary.State = EJobState::Completed;
    jobSummary.FinishTime = TInstant::Now();

    return std::make_unique<TCompletedJobSummary>(std::move(jobSummary));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSimulatorNodeShard::TSimulatorNodeShard(
    int shardId,
    TSharedEventQueue* events,
    TSchedulerStrategyHost* strategyHost,
    TSharedSchedulerStrategy* schedulingStrategy,
    TSharedOperationStatistics* operationStatistics,
    IOperationStatisticsOutput* operationStatisticsOutput,
    TSharedRunningOperationsMap* runningOperationsMap,
    TSharedJobAndOperationCounter* jobAndOperationCounter,
    TInstant earliestTime,
    TSchedulerSimulatorConfigPtr config,
    TSchedulerConfigPtr schedulerConfig)
    : Id_(shardId)
    , Events_(events)
    , StrategyHost_(strategyHost)
    , SchedulingStrategy_(schedulingStrategy)
    , OperationStatistics_(operationStatistics)
    , OperationStatisticsOutput_(operationStatisticsOutput)
    , RunningOperationsMap_(runningOperationsMap)
    , JobAndOperationCounter_(jobAndOperationCounter)
    , EarliestTime_(earliestTime)
    , Config_(std::move(config))
    , SchedulerConfig_(std::move(schedulerConfig))
    , ActionQueue_(New<TActionQueue>(Format("NodeShard:%v", Id_)))
    , Logger(SchedulerSimulatorLogger.WithTag("NodeShardId: %v", Id_))
    , MediumDirectory_(CreateDefaultMediumDirectory())
{
    if (Config_->RemoteEventLog) {
        RemoteEventLogWriter_ = CreateRemoteEventLogWriter(Config_->RemoteEventLog, GetInvoker());
        RemoteEventLogConsumer_ = RemoteEventLogWriter_->CreateConsumer();
    }
}

void TSimulatorNodeShard::RegisterNode(const NScheduler::TExecNodePtr& node)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    IdToNode_.emplace(node->GetId(), node);
}

void TSimulatorNodeShard::BuildNodesYson(TFluentMap fluent)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    for (const auto& [nodeId, node] : IdToNode_) {
        BuildNodeYson(node, fluent);
    }
}

void TSimulatorNodeShard::OnHeartbeat(const TNodeEvent& event)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    const auto& node = IdToNode_[event.NodeId];

    YT_LOG_DEBUG(
        "Heartbeat started "
        "(VirtualTimestamp: %v, NodeId: %v, NodeAddress: %v, "
        "ResourceUsage: %v, DiskResources: %v, JobCount: %v)",
        event.Time,
        event.NodeId,
        node->GetDefaultAddress(),
        NScheduler::FormatResourceUsage(
            TJobResources(node->GetResourceUsage()),
            TJobResources(node->GetResourceLimits())),
        node->GetDiskResources(),
        node->Allocations().size());

    // Prepare scheduling context.
    const auto& allocationsSet = node->Allocations();
    std::vector<TAllocationPtr> nodeAllocations(allocationsSet.begin(), allocationsSet.end());
    // NB(eshcherbin): We usually create a lot of simulator node shards running over a small thread pool to
    // introduce artificial contention. Thus we need to reduce the shard id to the range [0, MaxNodeShardCount).
    auto schedulingContext = New<TSchedulingContext>(Id_, SchedulerConfig_, node, nodeAllocations, MediumDirectory_);
    schedulingContext->SetNow(NProfiling::InstantToCpuInstant(event.Time));

    auto strategyProxy = SchedulingStrategy_->CreateNodeHeartbeatStrategyProxy(
        node->GetId(),
        node->GetDefaultAddress(),
        node->Tags(),
        node->GetMatchingTreeCookie());

    WaitFor(strategyProxy->ProcessSchedulingHeartbeat(schedulingContext, /*skipScheduleJobs*/ false))
        .ThrowOnError();

    node->SetResourceUsage(schedulingContext->ResourceUsage());

    // Create events for all started jobs.
    for (const auto& allocation : schedulingContext->StartedAllocations()) {
        const auto& duration = GetOrCrash(schedulingContext->GetStartedAllocationsDurations(), allocation->GetId());

        // Notify scheduler.
        allocation->SetState(EAllocationState::Running);

        YT_LOG_DEBUG(
            "Allocation started (VirtualTimestamp: %v, AllocationId: %v, OperationId: %v, FinishTime: %v, NodeId: %v)",
            event.Time,
            allocation->GetId(),
            allocation->GetOperationId(),
            event.Time + duration,
            event.NodeId);

        // Schedule new event.
        Events_->InsertNodeEvent(CreateAllocationFinishedNodeEvent(
            event.Time + duration,
            allocation,
            node,
            event.NodeId));

        // Update stats.
        OperationStatistics_->OnJobStarted(allocation->GetOperationId(), duration);

        EmplaceOrCrash(node->Allocations(), allocation);
        JobAndOperationCounter_->OnJobStarted();
    }

    // Process all preempted allocations.
    for (const auto& preemptedAllocation : schedulingContext->PreemptedAllocations()) {
        auto& allocation = preemptedAllocation.Allocation;
        auto duration = event.Time - allocation->GetStartTime();

        PreemptAllocation(allocation, Config_->EnableFullEventLog);
        auto operation = RunningOperationsMap_->Get(allocation->GetOperationId());
        auto controller = operation->GetControllerStrategyHost();
        controller->OnNonscheduledAllocationAborted(allocation->GetId(), EAbortReason::Preemption, TControllerEpoch{});

        // Update stats
        OperationStatistics_->OnJobPreempted(allocation->GetOperationId(), duration);

        JobAndOperationCounter_->OnJobPreempted();
    }

    if (!event.ScheduledOutOfBand) {
        auto nextHeartbeatEvent = event;
        nextHeartbeatEvent.Time += TDuration::MilliSeconds(Config_->HeartbeatPeriod);
        Events_->InsertNodeEvent(nextHeartbeatEvent);
    }

    TStringBuilder schedulingAttributesBuilder;
    TDelimitedStringBuilderWrapper delimitedSchedulingAttributesBuilder(&schedulingAttributesBuilder);
    strategyProxy->BuildSchedulingAttributesString(delimitedSchedulingAttributesBuilder);

    const auto& statistics = schedulingContext->GetSchedulingStatistics();
    YT_LOG_DEBUG(
        "Heartbeat finished "
        "(VirtualTimestamp: %v, NodeId: %v, NodeAddress: %v, "
        "StartedJobs: %v, PreemptedJobs: %v, "
        "AllocationsScheduledDuringPreemption: %v, UnconditionallyPreemptibleJobCount: %v, UnconditionalDiscount: %v, "
        "TotalConditionalAllocationCount: %v, MaxConditionalAllocationCountPerPool: %v, MaxConditionalDiscount: %v, "
        "ControllerScheduleAllocationCount: %v, ScheduleAllocationAttemptCountPerStage: %v, "
        "OperationCountByPreemptionPriority: %v, %v)",
        event.Time,
        event.NodeId,
        node->GetDefaultAddress(),
        schedulingContext->StartedAllocations().size(),
        schedulingContext->PreemptedAllocations().size(),
        statistics.ScheduledDuringPreemption,
        statistics.UnconditionallyPreemptibleAllocationCount,
        FormatResources(statistics.UnconditionalResourceUsageDiscount),
        statistics.TotalConditionallyPreemptibleAllocationCount,
        statistics.MaxConditionallyPreemptibleAllocationCountInPool,
        FormatResources(statistics.MaxConditionalResourceUsageDiscount),
        statistics.ControllerScheduleAllocationCount,
        statistics.ScheduleAllocationAttemptCountPerStage,
        statistics.OperationCountByPreemptionPriority,
        schedulingAttributesBuilder.Flush());
}

void TSimulatorNodeShard::OnAllocationFinished(const TNodeEvent& event)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto allocation = event.Allocation;

    // When job is aborted by scheduler, events list is not updated, so aborted
    // job will still have corresponding JobFinished event that should be ignored.
    if (allocation->GetState() != EAllocationState::Running) {
        return;
    }

    EraseOrCrash(allocation->GetNode()->Allocations(), allocation);

    YT_LOG_DEBUG(
        "Allocation finished (VirtualTimestamp: %v, AllocationId: %v, OperationId: %v, NodeId: %v)",
        event.Time,
        allocation->GetId(),
        allocation->GetOperationId(),
        event.NodeId);

    JobAndOperationCounter_->OnJobFinished();

    allocation->SetState(EAllocationState::Finished);

    if (Config_->EnableFullEventLog) {
        LogFinishedAllocationFluently(ELogEventType::JobCompleted, allocation);
    }

    auto jobSummary = BuildCompletedJobSummary(allocation);

    // Notify scheduler.
    auto operation = RunningOperationsMap_->Get(allocation->GetOperationId());
    auto operationController = operation->GetController();
    operationController->OnJobCompleted(std::move(jobSummary));
    if (operationController->IsOperationCompleted()) {
        operation->SetState(EOperationState::Completed);
    }

    std::vector<TAllocationUpdate> allocationUpdates({TAllocationUpdate{
        EAllocationUpdateStatus::Finished,
        allocation->GetOperationId(),
        allocation->GetId(),
        allocation->GetTreeId(),
        TJobResources(),
        /*allocationDataCenter*/ std::nullopt,
        /*allocationInfinibandCluster*/ std::nullopt,
    }});

    {
        THashSet<TAllocationId> allocationsToPostpone;
        THashMap<TAllocationId, EAbortReason> allocationsToAbort;
        SchedulingStrategy_->ProcessAllocationUpdates(
            allocationUpdates,
            &allocationsToPostpone,
            &allocationsToAbort);
        YT_VERIFY(allocationsToPostpone.empty());
        YT_VERIFY(allocationsToAbort.empty());
    }

    // Schedule out of band heartbeat.
    Events_->InsertNodeEvent(CreateHeartbeatNodeEvent(event.Time, event.NodeId, /*scheduledOutOfBand*/ true));

    // Update statistics.
    OperationStatistics_->OnJobFinished(operation->GetId(), event.Time - allocation->GetStartTime());

    const auto& node = IdToNode_[event.NodeId];
    YT_VERIFY(node == event.AllocationNode);
    node->SetResourceUsage(node->GetResourceUsage() - allocation->ResourceUsage());

    if (operation->GetState() == EOperationState::Completed && operation->SetCompleting()) {
        // Notify scheduler.
        SchedulingStrategy_->UnregisterOperation(operation.Get());

        RunningOperationsMap_->Erase(operation->GetId());

        JobAndOperationCounter_->OnOperationFinished();

        YT_LOG_INFO("Operation finished (VirtualTimestamp: %v, OperationId: %v)", event.Time, operation->GetId());

        const auto& id = operation->GetId();
        auto stats = OperationStatistics_->OnOperationFinished(
            id,
            operation->GetStartTime() - EarliestTime_,
            event.Time - EarliestTime_);
        OperationStatisticsOutput_->PrintEntry(id, std::move(stats));
    }
}

const IInvokerPtr& TSimulatorNodeShard::GetInvoker() const
{
    return ActionQueue_->GetInvoker();
}

void TSimulatorNodeShard::OnSimulationFinished()
{
    if (RemoteEventLogWriter_) {
        WaitFor(RemoteEventLogWriter_->Close())
            .ThrowOnError();
    }
}

int TSimulatorNodeShard::GetNodeShardId(TNodeId nodeId, int nodeShardCount)
{
    return THash<TNodeId>()(nodeId) % nodeShardCount;
}

void TSimulatorNodeShard::BuildNodeYson(const TExecNodePtr& node, TFluentMap fluent) const
{
    fluent
        .Item(node->GetDefaultAddress()).BeginMap()
            .Do([&] (TFluentMap fluent) {
                node->BuildAttributes(fluent);
            })
            .Do([&] (TFluentMap fluent) {
                SchedulingStrategy_->BuildSchedulingAttributesForNode(node->GetId(), node->GetDefaultAddress(), node->Tags(), fluent);
            })
        .EndMap();
}

void TSimulatorNodeShard::PreemptAllocation(const NScheduler::TAllocationPtr& allocation, bool shouldLogEvent)
{
    SchedulingStrategy_->PreemptAllocation(allocation);

    if (shouldLogEvent) {
        auto fluent = LogFinishedAllocationFluently(ELogEventType::JobAborted, allocation);
        if (auto preemptedFor = allocation->GetPreemptedFor()) {
            fluent
                .Item("preempted_for").Value(preemptedFor);
        }
    }
}

NYson::IYsonConsumer* TSimulatorNodeShard::GetEventLogConsumer()
{
    YT_VERIFY(RemoteEventLogConsumer_);
    return RemoteEventLogConsumer_.get();
}

const NLogging::TLogger* TSimulatorNodeShard::GetEventLogger()
{
    return nullptr;
}

NEventLog::TFluentLogEvent TSimulatorNodeShard::LogFinishedAllocationFluently(
    ELogEventType eventType,
    const TAllocationPtr& allocation)
{
    YT_LOG_INFO("Logging allocation event");

    return LogEventFluently(StrategyHost_->GetEventLogger(), eventType)
        .Item("allocation_id").Value(allocation->GetId())
        .Item("operation_id").Value(allocation->GetOperationId())
        .Item("start_time").Value(allocation->GetStartTime())
        .Item("resource_limits").Value(allocation->ResourceLimits());
}

int GetNodeShardId(TNodeId nodeId, int nodeShardCount)
{
    return THash<TNodeId>()(nodeId) % nodeShardCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
