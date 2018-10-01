#include "node_shard.h"
#include "operation_controller.h"

#include <yt/core/concurrency/scheduler.h>


namespace NYT {
namespace NSchedulerSimulator {

using namespace NScheduler;
using namespace NLogging;
using namespace NControllerAgent;
using namespace NConcurrency;

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

DEFINE_REFCOUNTED_TYPE(TNodeShard)

TNodeShard::TNodeShard(
    std::vector<TExecNodePtr>* execNodes,
    TSharedSchedulerEvents* events,
    TSharedSchedulingStrategy* schedulingData,
    TSharedOperationStatistics* operationsStatistics,
    TSharedOperationStatisticsOutput* operationStatisticsOutput,
    TSharedRunningOperationsMap* runningOperationsMap,
    TSharedJobAndOperationCounter* jobOperationCounter,
    const TSchedulerSimulatorConfigPtr& config,
    const TSchedulerConfigPtr& schedulerConfig,
    TInstant earliestTime,
    int workerId)
    : ExecNodes_(execNodes)
    , Events_(events)
    , SchedulingStrategy_(schedulingData)
    , OperationStatistics_(operationsStatistics)
    , OperationStatisticsOutput_(operationStatisticsOutput)
    , RunningOperationsMap_(runningOperationsMap)
    , JobAndOperationCounter_(jobOperationCounter)
    , Config_(config)
    , SchedulerConfig_(schedulerConfig)
    , EarliestTime_(earliestTime)
    , WorkerId_(workerId)
    , Logger(TLogger(NSchedulerSimulator::Logger)
        .AddTag("WorkerId: %v", workerId))
{ }


void TNodeShard::Run()
{
    int iter = 0;
    while (JobAndOperationCounter_->HasUnfinishedOperations()) {
        iter += 1;
        if (iter % Config_->CyclesPerFlush == 0) {
            LOG_INFO(
                "Simulated %v cycles (FinishedOperations: %v, RunningOperation: %v, "
                "TotalOperations: %v, RunningJobs: %v)",
                iter,
                JobAndOperationCounter_->GetFinishedOperationCount(),
                JobAndOperationCounter_->GetStartedOperationCount(),
                JobAndOperationCounter_->GetTotalOperationCount(),
                JobAndOperationCounter_->GetRunningJobCount());

            if (WorkerId_ == 0) {
                RunningOperationsMap_->ApplyRead([this] (const auto& pair) {
                    const auto& operation = pair.second;
                    LOG_INFO("%v, (OperationId: %v)",
                        operation->GetController()->GetLoggingProgress(),
                        operation->GetId());
                });
            }
        }

        RunOnce();
        Yield();
    }

    SchedulingStrategy_->OnSimulationFinished(WorkerId_);
}


void TNodeShard::RunOnce()
{
    auto eventNullable = Events_->PopEvent(WorkerId_);
    if (!eventNullable) {
        return;
    }
    auto event = eventNullable.Get();

    SchedulingStrategy_->OnEvent(WorkerId_, event);
    switch (event.Type) {
        case EEventType::Heartbeat: {
            OnHeartbeat(event);
            break;
        }

        case EEventType::OperationStarted: {
            OnOperationStarted(event);
            break;
        }

        case EEventType::JobFinished: {
            OnJobFinished(event);
            break;
        }
    }
}

void TNodeShard::OnHeartbeat(const TSchedulerEvent& event)
{
    LOG_DEBUG("Heartbeat started (NodeIndex: %v)", event.NodeIndex);

    auto& node = (*ExecNodes_)[event.NodeIndex];

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

        auto operation = RunningOperationsMap_->Get(job->GetOperationId());

        LOG_DEBUG("Job started (JobId: %v, OperationId: %v, FinishTime: %v, NodeIndex: %v)",
            job->GetId(),
            job->GetOperationId(),
            event.Time + duration,
            event.NodeIndex);

        // Schedule new event.
        auto jobFinishedEvent = TSchedulerEvent::JobFinished(
            event.Time + duration,
            job,
            node,
            event.NodeIndex);
        Events_->InsertEvent(WorkerId_, jobFinishedEvent);

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
        Events_->InsertEvent(WorkerId_, nextHeartbeat);
    }
    LOG_DEBUG("Heartbeat finished (NodeIndex: %v)", event.NodeIndex);
}

void TNodeShard::OnOperationStarted(const TSchedulerEvent& event)
{
    const auto& description = OperationStatistics_->GetOperationDescription(event.OperationId);

    auto runtimeParameters = New<TOperationRuntimeParameters>();
    SchedulingStrategy_->InitOperationRuntimeParameters(
        runtimeParameters,
        NYTree::ConvertTo<NScheduler::TOperationSpecBasePtr>(description.Spec),
        description.AuthenticatedUser,
        description.Type);
    auto operation = New<NSchedulerSimulator::TOperation>(description, runtimeParameters);

    auto operationController = CreateSimulatorOperationController(operation.Get(), &description);
    operation->SetController(operationController);

    RunningOperationsMap_->Insert(operation->GetId(), operation);
    OperationStatistics_->OnOperationStarted(operation->GetId());
    LOG_INFO("Operation started (OperationId: %v)", operation->GetId());

    // Notify scheduler.
    SchedulingStrategy_->RegisterOperation(operation.Get());
    SchedulingStrategy_->EnableOperation(operation.Get());

    JobAndOperationCounter_->OnOperationStarted();
}

void TNodeShard::OnJobFinished(const TSchedulerEvent& event)
{
    auto job = event.Job;

    // When job is aborted by scheduler, events list is not updated, so aborted
    // job will still have corresponding JobFinished event that should be ignored.
    if (job->GetState() != EJobState::Running) {
        return;
    }

    YCHECK(job->GetNode()->Jobs().erase(job) == 1);

    LOG_DEBUG(
        "Job finished (JobId: %v, OperationId: %v, NodeIndex: %v)",
        job->GetId(),
        job->GetOperationId(),
        event.NodeIndex);

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
    Events_->InsertEvent(WorkerId_, TSchedulerEvent::Heartbeat(event.Time, event.NodeIndex, true));

    // Update statistics.
    OperationStatistics_->OnJobFinished(operation->GetId(), event.Time - job->GetStartTime());

    auto& node = (*ExecNodes_)[event.NodeIndex];
    YCHECK(node == event.JobNode);
    node->SetResourceUsage(node->GetResourceUsage() - job->ResourceUsage());

    if (operation->GetState() == EOperationState::Completed && operation->SetCompleting()) {
        // Notify scheduler.
        SchedulingStrategy_->UnregisterOperation(operation.Get());

        RunningOperationsMap_->Erase(operation->GetId());

        JobAndOperationCounter_->OnOperationFinished();

        LOG_INFO("Operation finished (OperationId: %v)", operation->GetId());

        const auto& id = operation->GetId();
        auto stats = OperationStatistics_->OnOperationFinished(
            id,
            operation->GetStartTime() - EarliestTime_,
            event.Time - EarliestTime_);
        OperationStatisticsOutput_->PrintEntry(id, stats);
    }
}

} // namespace NSchedulerSimulator
} // namespace NYT
