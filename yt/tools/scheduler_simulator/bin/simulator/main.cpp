#include "private.h"
#include "config.h"
#include "operation.h"
#include "operation_controller.h"
#include "operation_description.h"
#include "scheduler_strategy_host.h"
#include "scheduling_context.h"
#include "shared_data.h"

#include <yt/server/scheduler/config.h>
#include <yt/server/scheduler/job.h>
#include <yt/server/scheduler/operation.h>

#include <yt/ytlib/job_tracker_client/public.h>
#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/core/misc/shutdown.h>

#include <yt/core/logging/config.h>
#include <yt/core/logging/log_manager.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/yson/lexer.h>
#include <yt/core/yson/parser.h>
#include <yt/core/yson/writer.h>
#include <yt/core/yson/null_consumer.h>
#include <yt/core/yson/stream.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/rw_spinlock.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/misc/property.h>

#include <util/stream/buffered.h>
#include <util/stream/file.h>

#include <fstream>
#include <atomic>

namespace NYT {

using namespace NSchedulerSimulator;
using namespace NNodeTrackerClient;
using namespace NYTree;
using namespace NPhoenix;
using namespace NJobTrackerClient;
using namespace NScheduler;
using namespace NControllerAgent;
using namespace NConcurrency;
using namespace NLogging;

static const auto& Logger = SchedulerSimulatorLogger;

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

TJobResources GetNodeResourceLimit(TNodeResourcesConfigPtr config)
{
    TJobResources resourceLimits;
    resourceLimits.SetMemory(config->Memory);
    resourceLimits.SetCpu(TCpuResource(config->Cpu));
    resourceLimits.SetUserSlots(config->UserSlots);
    resourceLimits.SetNetwork(config->Network);
    return resourceLimits;
}

TSchedulerSimulatorConfigPtr LoadConfig(const char* configFilename)
{
    INodePtr configNode;
    try {
        TIFStream configStream(configFilename);
        configNode = ConvertToNode(&configStream);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading scheduler simulator configuration") << ex;
    }

    auto config = New<TSchedulerSimulatorConfig>();
    config->Load(configNode);

    return config;
}

std::vector<TExecNodePtr> CreateExecNodes(const std::vector<TNodeGroupConfigPtr>& nodeGroups)
{
    std::vector<TExecNodePtr> execNodes;

    NNodeTrackerClient::NProto::TDiskResources diskResources;
    auto* diskReport = diskResources.add_disk_reports();
    diskReport->set_limit(100_GB);
    diskReport->set_usage(0);

    for (const auto& nodeGroupConfig : nodeGroups) {
        for (int i = 0; i < nodeGroupConfig->Count; ++i) {
            // NB: 0 is InvalidNodeId therefore we need +1.
            auto nodeId = execNodes.size() + 1;
            TNodeDescriptor descriptor("node" + ToString(nodeId));

            auto node = New<TExecNode>(nodeId, descriptor);
            node->Tags() = nodeGroupConfig->Tags;
            node->SetResourceLimits(GetNodeResourceLimit(nodeGroupConfig->ResourceLimits));
            node->SetDiskInfo(diskResources);
            execNodes.push_back(node);
        }
    }

    return execNodes;
}

std::vector<TOperationDescription> LoadOperations()
{
    std::vector<TOperationDescription> operations;
    {
        TLoadContext context;
        context.SetInput(&Cin);
        Load(context, operations);
    }
    return operations;
}

TOperationDescriptions CreateOperationDescriptions(const std::vector<TOperationDescription>& operations)
{
    TOperationDescriptions operationDescriptions;
    for (const auto& operation : operations) {
        operationDescriptions[operation.Id] = operation;
    }
    return operationDescriptions;
}

TInstant FindEarliestTime(const std::vector<TOperationDescription>& operations)
{
    auto earliestTime = TInstant::Max();
    for (const auto& operation : operations) {
        earliestTime = std::min(earliestTime, operation.StartTime);
    }
    return earliestTime;
}

} // namespace

DECLARE_REFCOUNTED_CLASS(TSimulatorWorker)

class TSimulatorWorker
    : public NYT::TRefCounted
{
public:
    TSimulatorWorker(
        std::vector<TExecNodePtr>* execNodes,
        TSharedSchedulerEvents* events,
        TSharedSchedulingStrategy* schedulingData,
        TSharedOperationStatistics* operationsStatistics,
        TSharedOperationStatisticsOutput* operationStatisticsOutput,
        TSharedRunningOperationsMap* runningOperationsMap,
        TSharedJobAndOperationCounter* jobOperationCounter,
        const TSchedulerSimulatorConfigPtr& config,
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
        , EarliestTime_(earliestTime)
        , WorkerId_(workerId)
        , Logger(TLogger(NYT::Logger)
            .AddTag("WorkerId: %v", workerId))
    { }

    void Run()
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

    void RunOnce()
    {
        auto eventNullable = Events_->PopEvent(WorkerId_);
        if (!eventNullable) {
            return;
        }
        auto event = eventNullable.Get();

        SchedulingStrategy_->OnEvent(WorkerId_, event);
        switch (event.Type) {
            case EEventType::Heartbeat: {
                LOG_DEBUG("Heartbeat started (NodeIndex: %v)", event.NodeIndex);

                auto& node = (*ExecNodes_)[event.NodeIndex];

                // Prepare scheduling context.
                const auto& jobsSet = node->Jobs();
                std::vector<TJobPtr> nodeJobs(jobsSet.begin(), jobsSet.end());
                auto context = New<TSchedulingContext>(Config_->SchedulerConfig, node, nodeJobs);
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
                break;
            }

            case EEventType::OperationStarted: {
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

                break;
            }

            case EEventType::JobFinished: {
                auto job = event.Job;

                // When job is aborted by scheduler, events list is not updated, so aborted
                // job will still have corresponding JobFinished event that should be ignored.
                if (job->GetState() != EJobState::Running) {
                    break;
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
                break;
            }
        }
    }

private:
    std::vector<TExecNodePtr>* ExecNodes_;
    TSharedSchedulerEvents* Events_;
    TSharedSchedulingStrategy* SchedulingStrategy_;
    TSharedOperationStatistics* OperationStatistics_;
    TSharedOperationStatisticsOutput* OperationStatisticsOutput_;
    TSharedRunningOperationsMap* RunningOperationsMap_;
    TSharedJobAndOperationCounter* JobAndOperationCounter_;

    const TSchedulerSimulatorConfigPtr Config_;
    const TInstant EarliestTime_;
    const int WorkerId_;

    TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TSimulatorWorker)


void Run(const char* configFilename)
{
    const auto config = LoadConfig(configFilename);

    TLogManager::Get()->Configure(config->Logging);

    LOG_INFO("Reading operations description");

    std::vector<TExecNodePtr> execNodes = CreateExecNodes(config->NodeGroups);

    LOG_INFO("Discovered %v nodes", execNodes.size());

    YCHECK(execNodes.size() > 0);

    const auto operations = LoadOperations();
    const auto operationDescriptions = CreateOperationDescriptions(operations);
    TSharedOperationStatistics operationStatistics(operationDescriptions);
    TSharedOperationStatisticsOutput operationStatisticsOutput(config->OperationsStatsFilename);
    TSharedRunningOperationsMap runningOperationsMap;

    const TInstant earliestTime = FindEarliestTime(operations);

    TSharedSchedulerEvents events(operations, config->HeartbeatPeriod, earliestTime, execNodes.size(), config->ThreadCount);
    TSharedJobAndOperationCounter jobAndOperationCounter(operations.size());

    TFixedBufferFileOutput eventLogFile(config->EventLogFilename);
    TSchedulerStrategyHost strategyHost(&execNodes, &eventLogFile);

    TThreadPoolPtr threadPool = New<TThreadPool>(config->ThreadCount, "Workers");
    auto invoker = threadPool->GetInvoker();

    TSharedSchedulingStrategy schedulingData(
        strategyHost,
        invoker,
        config,
        earliestTime,
        config->ThreadCount);

    LOG_INFO("Simulation started using %v threads", config->ThreadCount);

    operationStatisticsOutput.PrintHeader();

    std::vector<TFuture<void>> asyncWorkerResults;
    for (int workerId = 0; workerId < config->ThreadCount; ++workerId) {
        auto worker = New<TSimulatorWorker>(
            &execNodes,
            &events,
            &schedulingData,
            &operationStatistics,
            &operationStatisticsOutput,
            &runningOperationsMap,
            &jobAndOperationCounter,
            config,
            earliestTime,
            workerId);
        asyncWorkerResults.emplace_back(
            BIND(&TSimulatorWorker::Run, worker)
                .AsyncVia(invoker)
                .Run());
    }

    WaitFor(Combine(asyncWorkerResults))
        .ThrowOnError();

    LOG_INFO("Simulation finished");

    schedulingData.OnMasterDisconnected(invoker);
    threadPool->Shutdown();
}

} // namespace NYT


int main(int argc, char** argv)
{
    if (argc != 2) {
        printf("Usage: scheduler_simulator <config_filename>\n");
        return 1;
    }

    NYT::Run(argv[1]);
    NYT::Shutdown();

    return 0;
}
