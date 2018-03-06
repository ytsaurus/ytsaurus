#include "private.h"
#include "config.h"
#include "operation.h"
#include "operation_controller.h"
#include "operation_description.h"
#include "scheduler_strategy_host.h"
#include "scheduling_context.h"

#include <yt/server/scheduler/config.h>
#include <yt/server/scheduler/fair_share_strategy.h>
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

#include <util/stream/buffered.h>
#include <util/stream/file.h>

#include <fstream>

using namespace NYT;
using namespace NSchedulerSimulator;
using namespace NNodeTrackerClient;
using namespace NYTree;
using namespace NPhoenix;
using namespace NJobTrackerClient;
using namespace NScheduler;
using namespace NControllerAgent;

NLogging::TLogger Logger("Simulator");

DEFINE_ENUM(EEventType,
    (Heartbeat)
    (JobFinished)
    (OperationStarted)
);

struct TEvent
{
    EEventType Type;
    TInstant Time;
    NScheduler::TOperationId OperationId;
    int NodeIndex;
    TJobPtr Job;
    TExecNodePtr JobNode;
    bool ScheduledOutOfBand;

    TEvent(EEventType type, TInstant time)
        : Type(type)
        , Time(time)
        , OperationId(TGuid())
        , NodeIndex(-1)
        , Job(nullptr)
    { }

    static TEvent OperationStarted(TInstant time, NScheduler::TOperationId id)
    {
        TEvent event(EEventType::OperationStarted, time);
        event.OperationId = id;
        return event;
    }

    static TEvent Heartbeat(TInstant time, int nodeIndex, bool scheduledOutOfBand)
    {
        TEvent event(EEventType::Heartbeat, time);
        event.NodeIndex = nodeIndex;
        event.ScheduledOutOfBand = scheduledOutOfBand;
        return event;
    }

    static TEvent JobFinished(
        TInstant time,
        TJobPtr job,
        TExecNodePtr execNode,
        int nodeIndex)
    {
        TEvent event(EEventType::JobFinished, time);
        event.Job = job;
        event.JobNode = execNode;
        event.NodeIndex = nodeIndex;
        return event;
    }
};

bool operator < (const TEvent& lhs, const TEvent& rhs)
{
    return lhs.Time < rhs.Time;
}

TJobResources GetNodeResourceLimit(TNodeResourcesConfigPtr config)
{
    TJobResources resourceLimits;
    resourceLimits.SetMemory(config->Memory);
    resourceLimits.SetCpu(NScheduler::TCpuResource(config->Cpu));
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

INodePtr LoadPools(const TString& poolsFilename)
{
    try {
        TIFStream configStream(poolsFilename);
        return ConvertToNode(&configStream);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading pools configuration") << ex;
    }
}

void ConfigureLogging(NYT::NLogging::TLogConfigPtr configPtr)
{
    auto logManagerConfig = ConvertTo<NLogging::TLogConfigPtr>(std::move(configPtr));
    NLogging::TLogManager::Get()->Configure(std::move(logManagerConfig));
}

struct TOperationStats
{
    TOperationStats() = default;

    int JobCount = 0;
    int PreemptedJobCount = 0;
    // We want TDuration output format for this field.
    TDuration StartTime;
    TDuration FinishTime;
    TDuration RealDuration;
    TDuration JobMaxDuration;
    TDuration JobsTotalDuration;
    TDuration PreemptedJobsTotalDuration;
    EOperationType OperationType;
    TString OperationState;
    bool InTimeframe = true;
};

void PrintOperationsStatsHeader(std::ofstream& ofs)
{
    ofs << "id"
        << "," << "job_count"
        << "," << "preempted_job_count"
        << "," << "start_time"
        << "," << "finish_time"
        << "," << "real_duration"
        << "," << "jobs_total_duration"
        << "," << "job_max_duration"
        << "," << "preempted_jobs_total_duration"
        << "," << "operation_type"
        << "," << "operation_state"
        << "," << "in_timeframe"
        << std::endl;
}

void PrintOperationsStatsEntry(std::ofstream& ofs, const TGuid& id, const TOperationStats& stats)
{
    ofs << ToString(id)
        << "," << stats.JobCount
        << "," << stats.PreemptedJobCount
        << "," << stats.StartTime.ToString()
        << "," << stats.FinishTime.ToString()
        << "," << stats.RealDuration.ToString()
        << "," << stats.JobsTotalDuration.ToString()
        << "," << stats.JobMaxDuration.ToString()
        << "," << stats.PreemptedJobsTotalDuration.ToString()
        << "," << ToString(stats.OperationType)
        << "," << stats.OperationState
        << "," << stats.InTimeframe
        << std::endl;
}

void ExtractJobTypes(std::ostream os, const std::vector<TOperationDescription>& operations)
{
    for (const auto& operation : operations) {
        std::set<EJobType> jobTypes;
        for (const auto& job : operation.JobDescriptions) {
            jobTypes.insert(job.Type);
        }
        os << ToString(operation.Type) << ": [";
        for (const auto& type : jobTypes) {
            os << ToString(type) << "\t";
        }
        os << "]" << std::endl;
    }
}

void DoRun(const char* configFilename)
{
    auto config = LoadConfig(configFilename);

    NLogging::TLogManager::Get()->Configure(config->Logging);

    std::ofstream operationsStatsStream(config->OperationsStatsFilename);
    PrintOperationsStatsHeader(operationsStatsStream);

    LOG_WARNING("Reading operations description");

    std::vector<TExecNodePtr> execNodes;
    NNodeTrackerClient::NProto::TDiskResources diskResources;
    auto* report = diskResources.add_disk_reports();
    report->set_limit(100_GB);
    report->set_usage(0);
    for (const auto& nodeGroupConfig : config->NodeGroups) {
        for (int i = 0; i < nodeGroupConfig->Count; ++i) {
            auto nodeId = execNodes.size() + 1;  // NB: 0 is InvalidNodeId
            TNodeDescriptor descriptor("node" + ToString(nodeId));
            auto node = New<TExecNode>(nodeId, descriptor);
            node->Tags() = nodeGroupConfig->Tags;
            node->SetResourceLimits(GetNodeResourceLimit(nodeGroupConfig->ResourceLimits));
            node->SetDiskInfo(diskResources);
            execNodes.push_back(node);
        }
    }

    LOG_INFO("Discovered %v nodes in total", execNodes.size());

    YCHECK(execNodes.size() > 0);

    std::vector<TOperationDescription> operations;
    {
        TLoadContext context;
        context.SetInput(&Cin);
        Load(context, operations);
    }

    std::map<TGuid, TOperationDescription> operationsSet;
    for (const auto& operation : operations) {
        operationsSet[operation.Id] = operation;
    }

    auto earliestTime = TInstant::Max();

    std::multiset<TEvent> events;
    for (const auto& operation : operations) {
        events.insert(TEvent::OperationStarted(operation.StartTime, operation.Id));
        earliestTime = std::min(earliestTime, operation.StartTime);
    }

    auto heartbeatsStartTime = earliestTime - TDuration::MilliSeconds(config->HeartbeatPeriod);
    for (int i = 0; i < static_cast<int>(execNodes.size()); ++i) {
        auto heartbeatStartDelay = TDuration::MilliSeconds((config->HeartbeatPeriod * i) / execNodes.size());
        events.insert(TEvent::Heartbeat(heartbeatsStartTime + heartbeatStartDelay, i, false));
    }

    TUnbufferedFileOutput eventLogFile(config->EventLogFilename);
    TSchedulerStrategyHost strategyHost(execNodes, &eventLogFile);

    auto fairShareStrategy = CreateFairShareStrategy(config->SchedulerConfig, &strategyHost, {GetCurrentInvoker()});
    fairShareStrategy->UpdatePools(LoadPools(config->PoolsFilename));

    LOG_WARNING("Simulating");

    int iter = 1;

    int runningJobCount = 0;
    int startedOperationCount = 0;
    int finishedOperationCount = 0;

    std::map<NScheduler::TOperationId, TOperationStats> operationsStats;
    std::map<EEventType, int> eventsCounts;

    std::map<NScheduler::TOperationId, NSchedulerSimulator::TOperationPtr> runningOperations;

    auto minTime = TInstant::Max();
    auto maxTime = TInstant::Zero();

    auto lastFairShareUpdateTime = TInstant::Zero();
    auto lastFairShareLogTime = TInstant::Zero();

    auto fairShareUpdatePeriod = config->SchedulerConfig->FairShareUpdatePeriod;
    auto fairShareLogPeriod = config->SchedulerConfig->FairShareLogPeriod;

    while (finishedOperationCount < operations.size()) {
        YCHECK(events.size() >= execNodes.size());
        auto event = *events.begin();
        events.erase(events.begin());

        minTime = std::min(minTime, event.Time);
        maxTime = std::max(maxTime, event.Time);

        ++eventsCounts[event.Type];
        switch (event.Type) {
            case EEventType::Heartbeat: {
                auto& node = execNodes[event.NodeIndex];

                // Prepare scheduling context.
                const auto& jobsSet = node->Jobs();
                std::vector<TJobPtr> nodeJobs(jobsSet.begin(), jobsSet.end());
                auto context = New<TSchedulingContext>(config->SchedulerConfig, node, nodeJobs);
                context->SetNow(NProfiling::InstantToCpuInstant(event.Time));

                if (lastFairShareUpdateTime + fairShareUpdatePeriod < event.Time) {
                    fairShareStrategy->OnFairShareUpdateAt(event.Time);
                    lastFairShareUpdateTime = event.Time;
                }
                if (lastFairShareLogTime + fairShareLogPeriod < event.Time) {
                    if (config->EnableFullEventLog) {
                        fairShareStrategy->OnFairShareLoggingAt(event.Time);
                    } else {
                        fairShareStrategy->OnFairShareEssentialLoggingAt(event.Time);
                    }
                    lastFairShareLogTime = event.Time;
                }
                NConcurrency::WaitFor(fairShareStrategy->ScheduleJobs(context))
                    .ThrowOnError();

                node->SetResourceUsage(context->ResourceUsage());

                YCHECK(context->StartedJobs().size() == context->GetStartedJobsDurations().size());

                // Create events for all started jobs.
                for (int i = 0; i < context->StartedJobs().size(); ++i) {
                    const auto& job = context->StartedJobs()[i];
                    const auto& duration = context->GetStartedJobsDurations()[i];

                    // Notify scheduler.
                    job->SetState(EJobState::Running);

                    auto operation = runningOperations[job->GetOperationId()];

                    // Schedule new event.
                    events.insert(TEvent::JobFinished(
                        event.Time + duration,
                        job,
                        node,
                        event.NodeIndex));

                    // Update stats.
                    auto& stats = operationsStats[job->GetOperationId()];
                    ++stats.JobCount;
                    stats.JobMaxDuration = std::max(stats.JobMaxDuration, duration);

                    YCHECK(node->Jobs().insert(job).second);

                    runningJobCount += 1;
                }

                // Process all preempted jobs.
                for (const auto& job : context->PreemptedJobs()) {
                    job->SetFinishTime(event.Time);
                    auto duration = event.Time - job->GetStartTime();

                    strategyHost.PreemptJob(job, config->EnableFullEventLog);
                    auto operation = runningOperations[job->GetOperationId()];
                    auto controller = operation->GetControllerStrategyHost();
                    controller->OnNonscheduledJobAborted(job->GetId(), EAbortReason::Preemption);

                    // Update stats.
                    auto& stats = operationsStats[job->GetOperationId()];
                    --stats.JobCount;
                    ++stats.PreemptedJobCount;
                    stats.JobsTotalDuration += duration;
                    stats.PreemptedJobsTotalDuration += duration;

                    runningJobCount -= 1;
                }

                if (!event.ScheduledOutOfBand) {
                    auto nextHeartbeat = event;
                    nextHeartbeat.Time += TDuration::MilliSeconds(config->HeartbeatPeriod);
                    events.insert(nextHeartbeat);
                }
                break;
            }

            case EEventType::OperationStarted: {
                const auto& description = operationsSet[event.OperationId];

                auto operation = New<NSchedulerSimulator::TOperation>(description);
                auto operationController = New<NSchedulerSimulator::TOperationController>(operation.Get(), &description);
                operation->SetController(operationController);

                runningOperations[operation->GetId()] = operation;

                LOG_INFO("Operation started (OperationId: %v)", operation->GetId());

                // Notify scheduler.
                fairShareStrategy->RegisterOperation(operation.Get());
                fairShareStrategy->OnOperationRunning(operation->GetId());

                ++startedOperationCount;

                break;
            }

            case EEventType::JobFinished: {
                auto job = event.Job;
                auto operation = runningOperations[job->GetOperationId()];

                // When job is aborted by scheduler, events list is not updated, so aborted
                // job will still have corresponding JobFinished event that should be ignored.
                if (job->GetState() != EJobState::Running) {
                    continue;
                }

                runningJobCount -= 1;

                YCHECK(job->GetNode()->Jobs().erase(job) == 1);

                job->SetState(EJobState::Completed);
                job->SetFinishTime(event.Time);

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

                // Notify scheduler.
                auto operationController = operation->GetController();
                operationController->OnJobCompleted(std::make_unique<TCompletedJobSummary>(&jobEvent));
                if (operationController->IsOperationCompleted()) {
                    operation->SetState(NScheduler::EOperationState::Completed);
                }

                std::vector<NScheduler::TUpdatedJob> updateJobs;
                std::vector<NScheduler::TCompletedJob> completedJobs({
                    NScheduler::TCompletedJob(job->GetOperationId(), job->GetId(), job->GetTreeId())
                });
                std::vector<TJobId> jobsToAbort;
                fairShareStrategy->ProcessUpdatedAndCompletedJobs(
                    &updateJobs,
                    &completedJobs,
                    &jobsToAbort);
                YCHECK(updateJobs.empty());
                YCHECK(completedJobs.empty());
                YCHECK(jobsToAbort.empty());

                // Schedule out of band heartbeat.
                events.insert(TEvent::Heartbeat(event.Time, event.NodeIndex, true));

                // Update stats.
                auto& stats = operationsStats[operation->GetId()];
                stats.JobsTotalDuration += event.Time - job->GetStartTime();

                // Log operation completion.
                if (operation->GetState() == EOperationState::Completed) {
                    // Notify scheduler.
                    fairShareStrategy->UnregisterOperation(operation.Get());

                    runningOperations.erase(operation->GetId());
                    finishedOperationCount += 1;

                    LOG_DEBUG("Operation finished (OperationId: %v)", operation->GetId());

                    const auto& id = operation->GetId();
                    auto& stats = operationsStats[id];
                    stats.StartTime = operation->GetStartTime() - earliestTime;
                    stats.FinishTime = event.Time - earliestTime;
                    stats.RealDuration = operationsSet[id].Duration;
                    stats.OperationType = operationsSet[id].Type;
                    stats.OperationState = operationsSet[id].State;
                    stats.InTimeframe = operationsSet[id].InTimeframe;
                    PrintOperationsStatsEntry(operationsStatsStream, id, stats);
                }
                event.JobNode->SetResourceUsage(event.JobNode->GetResourceUsage() - job->ResourceUsage());
                break;
            }
        }

        iter += 1;
        if (iter % config->CyclesPerFlush == 0) {
            LOG_WARNING("Simulated %v cycles, time %v, completed operations %v"
                        ", running jobs %v, running operations %v",
                iter,
                ~ToString(event.Time),
                finishedOperationCount,
                runningJobCount,
                startedOperationCount - finishedOperationCount);

            for (const auto& eventCountPair : eventsCounts) {
                LOG_WARNING("Event %Qv was executed %v times",
                    eventCountPair.first,
                    eventCountPair.second);
            }
            eventsCounts.clear();
        }

        if (iter % 300000 == 0) {
            for (const auto& operation : runningOperations) {
                LOG_INFO("%v, Progress: %v, %v, (OperationId: %v)",
                    ~ToString(event.Time),
                    operation.second->GetController()->GetLoggingProgress(),
                    fairShareStrategy->GetOperationLoggingProgress(operation.second->GetId()),
                    operation.second->GetId());
            }
        }
    }

    LOG_WARNING("Duration: %v", (maxTime - minTime).Seconds());

    fairShareStrategy->OnMasterDisconnected();
}

void Run(const char* configFilename) {
    auto actionQueue = New<NConcurrency::TActionQueue>("Simulator");
    auto invoker = actionQueue->GetInvoker();

    BIND(DoRun, configFilename)
        .AsyncVia(invoker)
        .Run()
        .Get()
        .ThrowOnError();
}

int main(int argc, char** argv)
{
    if (argc != 2) {
        printf("Usage: scheduler_simulator <config_filename>\n");
        return 1;
    }

    Run(argv[1]);
    Shutdown();

    return 0;
}
