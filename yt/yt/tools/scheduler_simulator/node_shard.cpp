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

std::unique_ptr<TCompletedJobSummary> BuildCompletedJobSummary(const TJobPtr& job)
{
    TCompletedJobSummary jobSummary;
    jobSummary.Id = job->GetId();
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
        "ResourceUsage: %v, JobCount: %v)",
        event.Time,
        event.NodeId,
        node->GetDefaultAddress(),
        StrategyHost_->FormatResourceUsage(
            TJobResources(node->GetResourceUsage()),
            TJobResources(node->GetResourceLimits()),
            node->GetDiskResources()),
        node->Jobs().size());

    // Prepare scheduling context.
    const auto& jobsSet = node->Jobs();
    std::vector<TJobPtr> nodeJobs(jobsSet.begin(), jobsSet.end());
    // NB(eshcherbin): We usually create a lot of simulator node shards running over a small thread pool to
    // introduce artificial contention. Thus we need to reduce the shard id to the range [0, MaxNodeShardCount).
    auto schedulingContext = New<TSchedulingContext>(Id_, SchedulerConfig_, node, nodeJobs, MediumDirectory_);
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
    for (const auto& job : schedulingContext->StartedJobs()) {
        const auto& duration = GetOrCrash(schedulingContext->GetStartedJobsDurations(), job->GetId());

        // Notify scheduler.
        job->SetAllocationState(EAllocationState::Running);

        YT_LOG_DEBUG("Job started (VirtualTimestamp: %v, JobId: %v, OperationId: %v, FinishTime: %v, NodeId: %v)",
            event.Time,
            job->GetId(),
            job->GetOperationId(),
            event.Time + duration,
            event.NodeId);

        // Schedule new event.
        Events_->InsertNodeEvent(CreateJobFinishedNodeEvent(
            event.Time + duration,
            job,
            node,
            event.NodeId));

        // Update stats.
        OperationStatistics_->OnJobStarted(job->GetOperationId(), duration);

        YT_VERIFY(node->Jobs().insert(job).second);
        JobAndOperationCounter_->OnJobStarted();
    }

    // Process all preempted jobs.
    for (const auto& preemptedJob : schedulingContext->PreemptedJobs()) {
        auto& job = preemptedJob.Job;
        auto duration = event.Time - job->GetStartTime();

        PreemptJob(job, Config_->EnableFullEventLog);
        auto operation = RunningOperationsMap_->Get(job->GetOperationId());
        auto controller = operation->GetControllerStrategyHost();
        controller->OnNonscheduledJobAborted(job->GetId(), EAbortReason::Preemption, TControllerEpoch{});

        // Update stats
        OperationStatistics_->OnJobPreempted(job->GetOperationId(), duration);

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
        "JobsScheduledDuringPreemption: %v, UnconditionallyPreemptibleJobCount: %v, UnconditionalDiscount: %v, "
        "TotalConditionalJobCount: %v, MaxConditionalJobCountPerPool: %v, MaxConditionalDiscount: %v, "
        "ControllerScheduleJobCount: %v, ScheduleJobAttemptCountPerStage: %v, "
        "OperationCountByPreemptionPriority: %v, %v)",
        event.Time,
        event.NodeId,
        node->GetDefaultAddress(),
        schedulingContext->StartedJobs().size(),
        schedulingContext->PreemptedJobs().size(),
        statistics.ScheduledDuringPreemption,
        statistics.UnconditionallyPreemptibleJobCount,
        FormatResources(statistics.UnconditionalResourceUsageDiscount),
        statistics.TotalConditionallyPreemptibleJobCount,
        statistics.MaxConditionallyPreemptibleJobCountInPool,
        FormatResources(statistics.MaxConditionalResourceUsageDiscount),
        statistics.ControllerScheduleJobCount,
        statistics.ScheduleJobAttemptCountPerStage,
        statistics.OperationCountByPreemptionPriority,
        schedulingAttributesBuilder.Flush());
}

void TSimulatorNodeShard::OnJobFinished(const TNodeEvent& event)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto job = event.Job;

    // When job is aborted by scheduler, events list is not updated, so aborted
    // job will still have corresponding JobFinished event that should be ignored.
    if (job->GetAllocationState() != EAllocationState::Running) {
        return;
    }

    YT_VERIFY(job->GetNode()->Jobs().erase(job) == 1);

    YT_LOG_DEBUG(
        "Job finished (VirtualTimestamp: %v, JobId: %v, OperationId: %v, NodeId: %v)",
        event.Time,
        job->GetId(),
        job->GetOperationId(),
        event.NodeId);

    JobAndOperationCounter_->OnJobFinished();

    job->SetAllocationState(EAllocationState::Finished);

    if (Config_->EnableFullEventLog) {
        LogFinishedJobFluently(ELogEventType::JobCompleted, job);
    }

    auto jobSummary = BuildCompletedJobSummary(job);

    // Notify scheduler.
    auto operation = RunningOperationsMap_->Get(job->GetOperationId());
    auto operationController = operation->GetController();
    operationController->OnJobCompleted(std::move(jobSummary));
    if (operationController->IsOperationCompleted()) {
        operation->SetState(EOperationState::Completed);
    }

    std::vector<TJobUpdate> jobUpdates({TJobUpdate{
        EJobUpdateStatus::Finished,
        job->GetOperationId(),
        job->GetId(),
        job->GetTreeId(),
        TJobResources(),
        /*jobDataCenter*/ std::nullopt,
        /*jobInfinibandCluster*/ std::nullopt,
    }});

    {
        THashSet<TJobId> jobsToPostpone;
        std::vector<TJobId> jobsToAbort;
        SchedulingStrategy_->ProcessJobUpdates(
            jobUpdates,
            &jobsToPostpone,
            &jobsToAbort);
        YT_VERIFY(jobsToPostpone.empty());
        YT_VERIFY(jobsToAbort.empty());
    }

    // Schedule out of band heartbeat.
    Events_->InsertNodeEvent(CreateHeartbeatNodeEvent(event.Time, event.NodeId, /*scheduledOutOfBand*/ true));

    // Update statistics.
    OperationStatistics_->OnJobFinished(operation->GetId(), event.Time - job->GetStartTime());

    const auto& node = IdToNode_[event.NodeId];
    YT_VERIFY(node == event.JobNode);
    node->SetResourceUsage(node->GetResourceUsage() - job->ResourceUsage());

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

void TSimulatorNodeShard::PreemptJob(const NScheduler::TJobPtr& job, bool shouldLogEvent)
{
    SchedulingStrategy_->PreemptJob(job);

    if (shouldLogEvent) {
        auto fluent = LogFinishedJobFluently(ELogEventType::JobAborted, job);
        if (auto preemptedFor = job->GetPreemptedFor()) {
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

NEventLog::TFluentLogEvent TSimulatorNodeShard::LogFinishedJobFluently(ELogEventType eventType, const TJobPtr& job)
{
    YT_LOG_INFO("Logging job event");

    return LogEventFluently(StrategyHost_->GetEventLogger(), eventType)
        .Item("job_id").Value(job->GetId())
        .Item("operation_id").Value(job->GetOperationId())
        .Item("start_time").Value(job->GetStartTime())
        .Item("resource_limits").Value(job->ResourceLimits());
}

int GetNodeShardId(TNodeId nodeId, int nodeShardCount)
{
    return THash<TNodeId>()(nodeId) % nodeShardCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
