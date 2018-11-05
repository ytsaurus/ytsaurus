#include "node_shard.h"
#include "operation_controller.h"

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/yson/public.h>


namespace NYT {
namespace NSchedulerSimulator {

using namespace NScheduler;
using namespace NLogging;
using namespace NControllerAgent;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NNodeTrackerClient;

static const auto& Logger = SchedulerSimulatorLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

NScheduler::NProto::TSchedulerToAgentJobEvent BuildSchedulerToAgentJobEvent(const TJobPtr& job)
{
    NScheduler::NProto::TSchedulerToAgentJobEvent jobEvent;
    jobEvent.set_event_type(static_cast<int>(ESchedulerToAgentJobEventType::Completed));
    ToProto(jobEvent.mutable_operation_id(), job->GetOperationId());
    ToProto(jobEvent.mutable_status()->mutable_job_id(), job->GetId());
    ToProto(jobEvent.mutable_status()->mutable_operation_id(), job->GetOperationId());
    jobEvent.set_log_and_profile(true);
    jobEvent.mutable_status()->set_state(static_cast<int>(job->GetState()));
    jobEvent.set_start_time(ToProto<ui64>(job->GetStartTime()));
    jobEvent.set_finish_time(ToProto<ui64>(*job->GetFinishTime()));
    jobEvent.set_abandoned(false);
    jobEvent.set_interrupt_reason(static_cast<int>(EInterruptReason::None));
    return jobEvent;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TSimulatorNodeShard)

TSimulatorNodeShard::TSimulatorNodeShard(
    TSharedEventQueue* events,
    TSharedSchedulerStrategy* schedulingStrategy,
    TSharedOperationStatistics* operationStatistics,
    TSharedOperationStatisticsOutput* operationStatisticsOutput,
    TSharedRunningOperationsMap* runningOperationsMap,
    TSharedJobAndOperationCounter* jobAndOperationCounter,
    const TSchedulerSimulatorConfigPtr& config,
    const TSchedulerConfigPtr& schedulerConfig,
    TInstant earliestTime,
    int shardId)
    : Events_(events)
    , SchedulingStrategy_(schedulingStrategy)
    , OperationStatistics_(operationStatistics)
    , OperationStatisticsOutput_(operationStatisticsOutput)
    , RunningOperationsMap_(runningOperationsMap)
    , JobAndOperationCounter_(jobAndOperationCounter)
    , Config_(config)
    , SchedulerConfig_(schedulerConfig)
    , EarliestTime_(earliestTime)
    , ShardId_(shardId)
    , ActionQueue_(New<TActionQueue>(Format("NodeShard:%v", shardId)))
    , Logger(TLogger(NSchedulerSimulator::Logger)
        .AddTag("ShardId: %v", shardId))
{ }

const IInvokerPtr& TSimulatorNodeShard::GetInvoker() const
{
    return ActionQueue_->GetInvoker();
}

TFuture<void> TSimulatorNodeShard::AsyncRun()
{
    return BIND(&TSimulatorNodeShard::Run, MakeStrong(this))
        .AsyncVia(GetInvoker())
        .Run();
}

void TSimulatorNodeShard::RegisterNode(const NYT::NScheduler::TExecNodePtr& node)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    IdToNode_.emplace(node->GetId(), node);
}

void TSimulatorNodeShard::BuildNodesYson(TFluentMap fluent)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    for (const auto& pair : IdToNode_) {
        BuildNodeYson(pair.second, fluent);
    }
}

void TSimulatorNodeShard::Run()
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    while (JobAndOperationCounter_->HasUnfinishedOperations()) {
        RunOnce();
        Yield();
    }

    Events_->OnNodeShardSimulationFinished(ShardId_);
}

void TSimulatorNodeShard::RunOnce()
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto eventNullable = Events_->PopNodeShardEvent(ShardId_);
    if (!eventNullable) {
        return;
    }
    auto event = eventNullable.Get();

    switch (event.Type) {
        case EEventType::Heartbeat: {
            OnHeartbeat(event);
            break;
        }

        case EEventType::JobFinished: {
            OnJobFinished(event);
            break;
        }
    }
}

void TSimulatorNodeShard::OnHeartbeat(const TNodeShardEvent& event)
{
    LOG_DEBUG("Heartbeat started (VirtualTimestamp: %v, NodeId: %v)", event.Time, event.NodeId);

    const auto& node = IdToNode_[event.NodeId];

    // Prepare scheduling context.
    const auto& jobsSet = node->Jobs();
    std::vector<TJobPtr> nodeJobs(jobsSet.begin(), jobsSet.end());
    auto context = New<TSchedulingContext>(SchedulerConfig_, node, nodeJobs);
    context->SetNow(NProfiling::InstantToCpuInstant(event.Time));

    SchedulingStrategy_->ScheduleJobs(context);

    node->SetResourceUsage(context->ResourceUsage());

    // Create events for all started jobs.
    for (const auto& job : context->StartedJobs()) {
        const auto& durations = context->GetStartedJobsDurations();
        auto it = durations.find(job->GetId());
        YCHECK(it != durations.end());
        const auto& duration = it->second;

        // Notify scheduler.
        job->SetState(EJobState::Running);

        LOG_DEBUG("Job started (VirtualTimestamp: %v, JobId: %v, OperationId: %v, FinishTime: %v, NodeId: %v)",
            event.Time,
            job->GetId(),
            job->GetOperationId(),
            event.Time + duration,
            event.NodeId);

        // Schedule new event.
        auto jobFinishedEvent = TNodeShardEvent::JobFinished(
            event.Time + duration,
            job,
            node,
            event.NodeId);
        Events_->InsertNodeShardEvent(ShardId_, jobFinishedEvent);

        // Update stats.
        OperationStatistics_->OnJobStarted(job->GetOperationId(), duration);

        YCHECK(node->Jobs().insert(job).second);
        JobAndOperationCounter_->OnJobStarted();
    }

    // Process all preempted jobs.
    for (const auto& job : context->PreemptedJobs()) {
        job->SetFinishTime(event.Time);
        auto duration = event.Time - job->GetStartTime();

        SchedulingStrategy_->PreemptJob(job, Config_->EnableFullEventLog);
        auto operation = RunningOperationsMap_->Get(job->GetOperationId());
        auto controller = operation->GetControllerStrategyHost();
        controller->OnNonscheduledJobAborted(job->GetId(), EAbortReason::Preemption);

        // Update stats
        OperationStatistics_->OnJobPreempted(job->GetOperationId(), duration);

        JobAndOperationCounter_->OnJobPreempted();
    }

    if (!event.ScheduledOutOfBand) {
        auto nextHeartbeat = event;
        nextHeartbeat.Time += TDuration::MilliSeconds(Config_->HeartbeatPeriod);
        Events_->InsertNodeShardEvent(ShardId_, nextHeartbeat);
    }
    LOG_DEBUG("Heartbeat finished (VirtualTimestamp: %v, NodeId: %v)", event.Time, event.NodeId);
}

void TSimulatorNodeShard::OnJobFinished(const TNodeShardEvent& event)
{
    auto job = event.Job;

    // When job is aborted by scheduler, events list is not updated, so aborted
    // job will still have corresponding JobFinished event that should be ignored.
    if (job->GetState() != EJobState::Running) {
        return;
    }

    YCHECK(job->GetNode()->Jobs().erase(job) == 1);

    LOG_DEBUG(
        "Job finished (VirtualTimestamp: %v, JobId: %v, OperationId: %v, NodeId: %v)",
        event.Time,
        job->GetId(),
        job->GetOperationId(),
        event.NodeId);

    JobAndOperationCounter_->OnJobFinished();

    job->SetState(EJobState::Completed);
    job->SetFinishTime(event.Time);

    auto jobEvent = BuildSchedulerToAgentJobEvent(job);

    // Notify scheduler.
    auto operation = RunningOperationsMap_->Get(job->GetOperationId());
    auto operationController = operation->GetController();
    operationController->OnJobCompleted(std::make_unique<TCompletedJobSummary>(&jobEvent));
    if (operationController->IsOperationCompleted()) {
        operation->SetState(EOperationState::Completed);
    }

    std::vector<TJobUpdate> jobUpdates({TJobUpdate{
        EJobUpdateStatus::Finished,
        job->GetOperationId(),
        job->GetId(),
        job->GetTreeId(),
        TJobResources(),
    }});

    {
        std::vector<std::pair<TJobId, TOperationId>> jobsToRemove;
        std::vector<TJobId> jobsToAbort;
        SchedulingStrategy_->ProcessJobUpdates(
            jobUpdates,
            &jobsToRemove,
            &jobsToAbort);
        YCHECK(jobsToRemove.size() == 1);
        YCHECK(jobsToAbort.empty());
    }

    // Schedule out of band heartbeat.
    Events_->InsertNodeShardEvent(ShardId_, TNodeShardEvent::Heartbeat(event.Time, event.NodeId, true));

    // Update statistics.
    OperationStatistics_->OnJobFinished(operation->GetId(), event.Time - job->GetStartTime());

    const auto& node = IdToNode_[event.NodeId];
    YCHECK(node == event.JobNode);
    node->SetResourceUsage(node->GetResourceUsage() - job->ResourceUsage());

    if (operation->GetState() == EOperationState::Completed && operation->SetCompleting()) {
        // Notify scheduler.
        SchedulingStrategy_->UnregisterOperation(operation.Get());

        RunningOperationsMap_->Erase(operation->GetId());

        JobAndOperationCounter_->OnOperationFinished();

        LOG_INFO("Operation finished (VirtualTimestamp: %v, OperationId: %v)", event.Time, operation->GetId());

        const auto& id = operation->GetId();
        auto stats = OperationStatistics_->OnOperationFinished(
            id,
            operation->GetStartTime() - EarliestTime_,
            event.Time - EarliestTime_);
        OperationStatisticsOutput_->PrintEntry(id, stats);
    }
}

void TSimulatorNodeShard::BuildNodeYson(const TExecNodePtr& node, TFluentMap fluent)
{
    fluent
        .Item(node->GetDefaultAddress()).BeginMap()
        .Do([&] (TFluentMap fluent) {
            BuildExecNodeAttributes(node, fluent);
        })
        .EndMap();
}

int GetNodeShardId(TNodeId nodeId, int nodeShardCount)
{
    return THash<TNodeId>()(nodeId) % nodeShardCount;
}

} // namespace NSchedulerSimulator
} // namespace NYT
