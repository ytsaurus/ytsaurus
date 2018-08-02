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

TLogger Logger("Simulator");

DEFINE_ENUM(EEventType,
    (Heartbeat)
    (JobFinished)
    (OperationStarted)
    (NoEvents)
);

struct TSchedulerEvent
{
    EEventType Type;
    TInstant Time;
    NScheduler::TOperationId OperationId;
    int NodeIndex;
    TJobPtr Job;
    TExecNodePtr JobNode;
    bool ScheduledOutOfBand;

    TSchedulerEvent(EEventType type, TInstant time)
        : Type(type)
        , Time(time)
        , OperationId(TGuid())
        , NodeIndex(-1)
        , Job(nullptr)
    { }

    static TSchedulerEvent OperationStarted(TInstant time, NScheduler::TOperationId id)
    {
        TSchedulerEvent event(EEventType::OperationStarted, time);
        event.OperationId = id;
        return event;
    }

    static TSchedulerEvent Heartbeat(TInstant time, int nodeIndex, bool scheduledOutOfBand)
    {
        TSchedulerEvent event(EEventType::Heartbeat, time);
        event.NodeIndex = nodeIndex;
        event.ScheduledOutOfBand = scheduledOutOfBand;
        return event;
    }

    static TSchedulerEvent JobFinished(
        TInstant time,
        TJobPtr job,
        TExecNodePtr execNode,
        int nodeIndex)
    {
        TSchedulerEvent event(EEventType::JobFinished, time);
        event.Job = job;
        event.JobNode = execNode;
        event.NodeIndex = nodeIndex;
        return event;
    }

    static TSchedulerEvent NoEvents()
    {
        TSchedulerEvent event(EEventType::NoEvents, { });
        return event;
    }
};


bool operator < (const TSchedulerEvent& lhs, const TSchedulerEvent& rhs)
{
    return lhs.Time < rhs.Time;
}

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

INodePtr LoadPoolTrees(const TString& poolTreesFilename)
{
    try {
        TIFStream configStream(poolTreesFilename);
        return ConvertToNode(&configStream);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading pool trees ") << ex;
    }
}

void ConfigureLogging(TLogConfigPtr configPtr)
{
    auto logManagerConfig = ConvertTo<TLogConfigPtr>(std::move(configPtr));
    TLogManager::Get()->Configure(std::move(logManagerConfig));
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

class TSharedOperationStatisticsOutput
{
public:
    explicit TSharedOperationStatisticsOutput(const TString& filename)
        : OutputStream_(filename)
    { }

    void PrintHeader()
    {
        auto guard = Guard(Lock_);
        OutputStream_
            << "id"
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

    void PrintEntry(const TOperationId& id, const TOperationStats& stats)
    {
        auto guard = Guard(Lock_);
        OutputStream_
            << ToString(id)
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

private:
    std::ofstream OutputStream_;
    TAdaptiveLock Lock_;
};

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

THashMap<TOperationId, TOperationDescription> CreateOperationsSet(const std::vector<TOperationDescription>& operations)
{
    THashMap<TOperationId, TOperationDescription> operationsSet;
    for (const auto& operation : operations) {
        operationsSet[operation.Id] = operation;
    }
    return operationsSet;
}

class TSharedSchedulerEvents
{
public:
    TSharedSchedulerEvents(
        const std::vector<TOperationDescription>& operations,
        int heartbeatPeriod,
        TInstant earliestTime,
        int execNodesCount)
    {
        for (const auto& operation : operations) {
            Events_.insert(TSchedulerEvent::OperationStarted(operation.StartTime, operation.Id));
        }
        const auto heartbeatsStartTime = earliestTime - TDuration::MilliSeconds(heartbeatPeriod);
        for (int i = 0; i < static_cast<int>(execNodesCount); ++i) {
            const auto heartbeatStartDelay = TDuration::MilliSeconds((heartbeatPeriod * i) / execNodesCount);
            Events_.insert(TSchedulerEvent::Heartbeat(heartbeatsStartTime + heartbeatStartDelay, i, false));
        }
    }

    void InsertEvent(TSchedulerEvent event)
    {
        auto guard = Guard(Lock_);
        Events_.insert(std::move(event));
    }

    TSchedulerEvent PopEvent()
    {
        auto guard = Guard(Lock_);
        if (Events_.empty()) {
            return TSchedulerEvent::NoEvents();
        }
        auto beginIt = Events_.begin();
        TSchedulerEvent event = *beginIt;
        Events_.erase(beginIt);
        return event;
    }

private:
    std::multiset<TSchedulerEvent> Events_;
    TAdaptiveLock Lock_;
};

class TSharedSchedulingStrategy
{
public:
    TSharedSchedulingStrategy(
        TSchedulerStrategyHost& strategyHost,
        const IInvokerPtr& invoker,
        const TSchedulerSimulatorConfigPtr config)
        : StrategyHost_(strategyHost)
        , LastFairShareUpdateTime_(TInstant::Zero())
        , LastFairShareLogTime_(TInstant::Zero())
        , FairShareUpdatePeriod_(config->SchedulerConfig->FairShareUpdatePeriod)
        , FairShareLogPeriod_(config->SchedulerConfig->FairShareLogPeriod)
        , EnableFullEventLog_(config->EnableFullEventLog)
    {
       SchedulerStrategy_ = CreateFairShareStrategy(config->SchedulerConfig, &strategyHost, {invoker});
       WaitFor(
           BIND(&ISchedulerStrategy::UpdatePoolTrees, SchedulerStrategy_, LoadPoolTrees(config->PoolTreesFilename))
               .AsyncVia(invoker)
               .Run())
           .ThrowOnError();
    }

    void PreemptJob(const NScheduler::TJobPtr& job, bool shouldLogEvent)
    {
        auto guard = Guard(Lock_);
        StrategyHost_.PreemptJob(job, shouldLogEvent);
    }

#define SchedulerStrategyOpProxy(OpName) \
    template <typename... T> auto OpName(T&&... args) \
    { \
        auto guard = Guard(Lock_); \
        return SchedulerStrategy_->OpName(std::forward<T>(args)...); \
    }

    SchedulerStrategyOpProxy(OnFairShareUpdateAt)
    SchedulerStrategyOpProxy(OnFairShareLoggingAt)
    SchedulerStrategyOpProxy(OnFairShareEssentialLoggingAt)
    SchedulerStrategyOpProxy(RegisterOperation)
    SchedulerStrategyOpProxy(EnableOperation)
    SchedulerStrategyOpProxy(ProcessJobUpdates)
    SchedulerStrategyOpProxy(UnregisterOperation)

#undef SchedulerStrategyOpProxy

    void ScheduleJobs(const ISchedulingContextPtr& schedulingContext)
    {
        WaitFor(SchedulerStrategy_->ScheduleJobs(schedulingContext))
            .ThrowOnError();
    }

    void OnMasterDisconnected(const IInvokerPtr& invoker)
    {
        auto guard = Guard(Lock_);
        WaitFor(
            BIND(&ISchedulerStrategy::OnMasterDisconnected, SchedulerStrategy_)
                .AsyncVia(invoker)
                .Run())
            .ThrowOnError();
    }

    void OnHeartbeat(const TSchedulerEvent& event)
    {
        auto guard = Guard(Lock_);
        YCHECK(event.Type == EEventType::Heartbeat);
        if (LastFairShareUpdateTime_ + FairShareUpdatePeriod_ < event.Time) {
            SchedulerStrategy_->OnFairShareUpdateAt(event.Time);
            LastFairShareUpdateTime_ = event.Time;
        }
        if (LastFairShareLogTime_ + FairShareLogPeriod_ < event.Time) {
            if (EnableFullEventLog_) {
                SchedulerStrategy_->OnFairShareLoggingAt(event.Time);
            } else {
                SchedulerStrategy_->OnFairShareEssentialLoggingAt(event.Time);
            }
            LastFairShareLogTime_ = event.Time;
        }
    }

private:
    TSchedulerStrategyHost& StrategyHost_;
    ISchedulerStrategyPtr SchedulerStrategy_;
    TInstant LastFairShareUpdateTime_;
    TInstant LastFairShareLogTime_;
    const TDuration FairShareUpdatePeriod_;
    const TDuration FairShareLogPeriod_;
    const bool EnableFullEventLog_;
    TAdaptiveLock Lock_;
};

class TSharedOperationStorage
{
public:
    explicit TSharedOperationStorage(const THashMap<TOperationId, TOperationDescription>& operationsSet)
        : OperationsSet_(operationsSet)
    { }

    TOperationStats OnJobStarted(const TOperationId& operationId, TDuration duration)
    {
        auto guard = Guard(Lock_);
        auto& stats = OperationsStorage_[operationId];
        ++stats.JobCount;
        stats.JobMaxDuration = std::max(stats.JobMaxDuration, duration);
        return stats;
    }

    TOperationStats OnJobPreempted(const TOperationId& operationId, TDuration duration)
    {
        auto guard = Guard(Lock_);
        auto& stats = OperationsStorage_[operationId];
        --stats.JobCount;
        ++stats.PreemptedJobCount;
        stats.JobsTotalDuration += duration;
        stats.PreemptedJobsTotalDuration += duration;
        return stats;
    }

    TOperationStats OnJobFinished(const TOperationId& operationId, TDuration duration)
    {
        auto guard = Guard(Lock_);
        auto& stats = OperationsStorage_[operationId];
        stats.JobsTotalDuration += duration;
        return stats;
    }

    TOperationStats OnOperationFinished(const TOperationId& operationId, TDuration startTime, TDuration finishTime)
    {
        auto guard = Guard(Lock_);
        auto& stats = OperationsStorage_[operationId];
        stats.StartTime = startTime;
        stats.FinishTime = finishTime;
        auto it = OperationsSet_.find(operationId);
        YCHECK(it != OperationsSet_.end());
        stats.RealDuration = it->second.Duration;
        stats.OperationType = it->second.Type;
        stats.OperationState = it->second.State;
        stats.InTimeframe = it->second.InTimeframe;
        return stats;
    }

    NSchedulerSimulator::TOperationPtr GetRunningOperation(const TOperationId& operationId)
    {
        auto guard = Guard(Lock_);
        auto it = RunningOperations_.find(operationId);
        YCHECK(it != RunningOperations_.end());
        return it->second;
    }

    void SetRunningOperation(NSchedulerSimulator::TOperationPtr operation)
    {
        auto guard = Guard(Lock_);
        RunningOperations_.emplace(operation->GetId(), operation);
    }

    void EraseRunningOperation(const TOperationId& operationId)
    {
        auto guard = Guard(Lock_);
        auto count = RunningOperations_.erase(operationId);
        YCHECK(count > 0);
    }

    const TOperationDescription& GetOperationDescription(const TOperationId& operationId) const
    {
        auto it = OperationsSet_.find(operationId);
        YCHECK(it != OperationsSet_.end());
        return it->second;
    }

private:
    std::map<TOperationId, TOperationStats> OperationsStorage_;
    std::map<TOperationId, NSchedulerSimulator::TOperationPtr> RunningOperations_;
    const THashMap<TOperationId, TOperationDescription>& OperationsSet_;
    TAdaptiveLock Lock_;
};


TInstant FindEarliestTime(const std::vector<TOperationDescription>& operations)
{
    auto earliestTime = TInstant::Max();
    for (const auto& operation : operations) {
        earliestTime = std::min(earliestTime, operation.StartTime);
    }
    return earliestTime;
}

class TSharedJobAndOperationCounters
{
public:
    explicit TSharedJobAndOperationCounters(int totalOperationCount)
        : RunningJobCount_(0)
        , StartedOperationCount_(0)
        , FinishedOperationCount_(0)
        , TotalOperationCount_(totalOperationCount)
    { }

    void OnJobStarted()
    {
        ++RunningJobCount_;
    }

    void OnJobPreempted()
    {
        --RunningJobCount_;
    }

    void OnJobFinished()
    {
        --RunningJobCount_;
    }

    void OnOperationStarted()
    {
        ++StartedOperationCount_;
    }

    void OnOperationFinished()
    {
        ++FinishedOperationCount_;
    }

    int GetRunningJobCount() const
    {
        return RunningJobCount_.load();
    }

    int GetStartedOperationCount() const
    {
        return StartedOperationCount_.load();
    }

    int GetFinishedOperationCount() const
    {
        return FinishedOperationCount_.load();
    }

    int GetTotalOperationCount() const
    {
        return TotalOperationCount_;
    }

    bool HasUnfinishedOperations() const
    {
        return FinishedOperationCount_ < TotalOperationCount_;
    }

private:
    std::atomic<int> RunningJobCount_;
    std::atomic<int> StartedOperationCount_;
    std::atomic<int> FinishedOperationCount_;
    const int TotalOperationCount_;
};

class TSchedulerEventsCounter
{
public:
    void OnEvent(const TSchedulerEvent& event)
    {
        auto guard = Guard(Lock_);
        ++EventsCount_[event.Type];
    }

    std::map<EEventType, int> GetStatistics() const
    {
        auto guard = Guard(Lock_);
        return EventsCount_;
    }

    void Reset()
    {
        auto guard = Guard(Lock_);
        EventsCount_.clear();
    }

private:
    std::map<EEventType, int> EventsCount_;
    TAdaptiveLock Lock_;
};

DECLARE_REFCOUNTED_CLASS(TSimulatorWorker)

class TSimulatorWorker
    : public NYT::TRefCounted
{
public:
    TSimulatorWorker(
        std::vector<TExecNodePtr>* execNodes,
        std::vector<TAdaptiveLock>* execNodeLocks,
        TSharedSchedulerEvents* events,
        TSharedSchedulingStrategy* schedulingData,
        TSharedOperationStorage* operationStorage,
        TSharedOperationStatisticsOutput* operationStatisticsOutput,
        TSharedJobAndOperationCounters* jobOperationCounter,
        const TSchedulerSimulatorConfigPtr& config,
        TInstant earliestTime,
        int workerId)
        : ExecNodes_(execNodes)
        , ExecNodeLocks_(execNodeLocks)
        , Events_(events)
        , SchedulingStrategy_(schedulingData)
        , OperationsStorage_(operationStorage)
        , OperationsStatisticsOutput_(operationStatisticsOutput)
        , JobAndOperationCounter_(jobOperationCounter)
        , Config_(config)
        , EarliestTime_(earliestTime)
        , Logger(TLogger(NYT::Logger)
            .AddTag("WorkerId: %v", workerId))
    { }

    void Run()
    {
        int iter = 0;
        while (JobAndOperationCounter_->HasUnfinishedOperations()) {
            iter += 1;
            if (iter % Config_->CyclesPerFlush == 0) {
                LOG_INFO("Simulated %v cycles (FinishedOperations: %v, RunningOperation: %v, "
                    "TotalOperations: %v, RunningJobs: %v)",
                    iter,
                    JobAndOperationCounter_->GetFinishedOperationCount(),
                    JobAndOperationCounter_->GetStartedOperationCount(),
                    JobAndOperationCounter_->GetTotalOperationCount(),
                    JobAndOperationCounter_->GetRunningJobCount());

                /*for (const auto& eventCountPair : eventsCounts) {
                    LOG_WARNING("Event %Qv was executed %v times",
                        eventCountPair.first,
                        eventCountPair.second);
                }
                eventsCounts.clear();*/
            }

            /*if (iter % 300000 == 0) {
                for (const auto& operation : runningOperations) {
                    LOG_INFO("%v, Progress: %v, %v, (OperationId: %v)",
                        ~ToString(event.Time),
                        operation.second->GetController()->GetLoggingProgress(),
                        // It is intentionally broken. We should use separate logging for each tree.
                        // SchedulingData->GetOperationLoggingProgress(operation.second->GetId()),
                        operation.second->GetId());
                }
            }*/

            RunOnce();
            Yield();

            //auto future = BIND(&TSimulatorWorker::RunOnce, MakeStrong(this)).AsyncVia(GetCurrentInvoker()).Run();
            //WaitFor(future).ThrowOnError();
        }
    }

    void RunOnce()
    {
        TSchedulerEvent event = Events_->PopEvent();

        switch (event.Type) {
            case EEventType::NoEvents: {
                LOG_DEBUG("No events to simulate");
                break;
            }

            case EEventType::Heartbeat: {
                LOG_DEBUG("Heartbeat started");

                SchedulingStrategy_->OnHeartbeat(event);

                auto guardNode = Guard((*ExecNodeLocks_)[event.NodeIndex]);
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

                    auto operation = OperationsStorage_->GetRunningOperation(job->GetOperationId());

                    LOG_DEBUG("Job started (JobId: %v, OperationId: %v, FinishTime: %v, Node: %v)",
                        job->GetId(),
                        job->GetOperationId(),
                        event.Time + duration,
                        node->GetDefaultAddress());

                    // Schedule new event.
                    Events_->InsertEvent(TSchedulerEvent::JobFinished(
                        event.Time + duration,
                        job,
                        node,
                        event.NodeIndex));

                    // Update stats.
                    OperationsStorage_->OnJobStarted(job->GetOperationId(), duration);

                    YCHECK(node->Jobs().insert(job).second);
                    JobAndOperationCounter_->OnJobStarted();
                }

                // Process all preempted jobs.
                for (const auto& job : context->PreemptedJobs()) {
                    job->SetFinishTime(event.Time);
                    auto duration = event.Time - job->GetStartTime();

                    SchedulingStrategy_->PreemptJob(job, Config_->EnableFullEventLog);
                    auto operation = OperationsStorage_->GetRunningOperation(job->GetOperationId());
                    auto controller = operation->GetControllerStrategyHost();
                    controller->OnNonscheduledJobAborted(job->GetId(), EAbortReason::Preemption);

                    // Update stats
                    OperationsStorage_->OnJobPreempted(job->GetOperationId(), duration);

                    JobAndOperationCounter_->OnJobPreempted();
                }

                if (!event.ScheduledOutOfBand) {
                    auto nextHeartbeat = event;
                    nextHeartbeat.Time += TDuration::MilliSeconds(Config_->HeartbeatPeriod);
                    Events_->InsertEvent(nextHeartbeat);
                }
                LOG_DEBUG("Heartbeat finished");
                break;
            }

            case EEventType::OperationStarted: {
                const auto& description = OperationsStorage_->GetOperationDescription(event.OperationId);

                auto operation = New<NSchedulerSimulator::TOperation>(description);

                auto operationController = New<NSchedulerSimulator::TOperationController>(operation.Get(), &description);
                operation->SetController(operationController);

                OperationsStorage_->SetRunningOperation(operation);

                LOG_INFO("Operation started (OperationId: %v)", operation->GetId());

                // Notify scheduler.
                SchedulingStrategy_->RegisterOperation(operation.Get());
                SchedulingStrategy_->EnableOperation(operation.Get());

                JobAndOperationCounter_->OnOperationStarted();

                break;
            }

            case EEventType::JobFinished: {
                auto job = event.Job;

                LOG_DEBUG("Job finished (JobId: %v, OperationId: %v)", job->GetId(), job->GetOperationId());

                // When job is aborted by scheduler, events list is not updated, so aborted
                // job will still have corresponding JobFinished event that should be ignored.
                if (job->GetState() != EJobState::Running) {
                    break;
                }

                JobAndOperationCounter_->OnJobFinished();

                {
                    auto guardNode = Guard((*ExecNodeLocks_)[event.NodeIndex]);
                    YCHECK(job->GetNode()->Jobs().erase(job) == 1);
                }

                job->SetState(EJobState::Completed);
                job->SetFinishTime(event.Time);

                auto jobEvent = BuildSchedulerToAgentJobEvent(job);

                // Notify scheduler.
                auto operation = OperationsStorage_->GetRunningOperation(job->GetOperationId());
                auto operationController = operation->GetController();
                operationController->OnJobCompleted(std::make_unique<TCompletedJobSummary>(&jobEvent));
                if (operationController->IsOperationCompleted()) {
                    operation->SetState(NScheduler::EOperationState::Completed);
                }

                std::vector<NScheduler::TJobUpdate> jobUpdates({TJobUpdate{
                    NScheduler::EJobUpdateStatus::Finished,
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
                Events_->InsertEvent(TSchedulerEvent::Heartbeat(event.Time, event.NodeIndex, true));

                // Update statistics.
                OperationsStorage_->OnJobFinished(operation->GetId(), event.Time - job->GetStartTime());

                {
                    auto guardNode = Guard((*ExecNodeLocks_)[event.NodeIndex]);
                    auto& node = (*ExecNodes_)[event.NodeIndex];
                    YCHECK(node == event.JobNode);
                    node->SetResourceUsage(node->GetResourceUsage() - job->ResourceUsage());
                }

                if (operation->GetState() == EOperationState::Completed && operation->SetCompleting()) {
                    // Notify scheduler.
                    SchedulingStrategy_->UnregisterOperation(operation.Get());

                    OperationsStorage_->EraseRunningOperation(operation->GetId());

                    JobAndOperationCounter_->OnOperationFinished();

                    LOG_DEBUG("Operation finished (OperationId: %v)", operation->GetId());

                    const auto& id = operation->GetId();
                    auto stats = OperationsStorage_->OnOperationFinished(
                        id,
                        operation->GetStartTime() - EarliestTime_,
                        event.Time - EarliestTime_);
                    OperationsStatisticsOutput_->PrintEntry(id, stats);
                }
                break;
            }
        }
    }

private:
    std::vector<TExecNodePtr>* ExecNodes_;
    std::vector<TAdaptiveLock>* ExecNodeLocks_;
    TSharedSchedulerEvents* Events_;
    TSharedSchedulingStrategy* SchedulingStrategy_;
    TSharedOperationStorage* OperationsStorage_;
    TSharedOperationStatisticsOutput* OperationsStatisticsOutput_;
    TSharedJobAndOperationCounters* JobAndOperationCounter_;

    const TSchedulerSimulatorConfigPtr Config_;
    const TInstant EarliestTime_;

    TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TSimulatorWorker)


void Run(const char* configFilename)
{
    const auto config = LoadConfig(configFilename);

    TLogManager::Get()->Configure(config->Logging);

    LOG_INFO("Reading operations description");

    std::vector<TExecNodePtr> execNodes = CreateExecNodes(config->NodeGroups);
    std::vector<TAdaptiveLock> execNodeLocks(execNodes.size());

    LOG_INFO("Discovered %v nodes", execNodes.size());

    YCHECK(execNodes.size() > 0);

    const auto operations = LoadOperations();
    const auto operationsSet = CreateOperationsSet(operations);
    TSharedOperationStorage operationStorage(operationsSet);
    TSharedOperationStatisticsOutput operationsStatisticsOutput(config->OperationsStatsFilename);

    const TInstant earliestTime = FindEarliestTime(operations);

    TSharedSchedulerEvents events(operations, config->HeartbeatPeriod, earliestTime, execNodes.size());
    TSharedJobAndOperationCounters jobAndOperationCounter(operations.size());

    TUnbufferedFileOutput eventLogFile(config->EventLogFilename);
    TSchedulerStrategyHost strategyHost(execNodes, &eventLogFile);

    TThreadPoolPtr threadPool = New<TThreadPool>(config->ThreadCount, "Workers");
    auto invoker = threadPool->GetInvoker();

    TSharedSchedulingStrategy schedulingData(
        strategyHost,
        invoker,
        config);

    LOG_INFO("Simulation started");

    operationsStatisticsOutput.PrintHeader();

    std::vector<TFuture<void>> asyncWorkerResults;
    for (int workerId = 0; workerId < config->ThreadCount; ++workerId) {
        auto worker = New<TSimulatorWorker>(
            &execNodes,
            &execNodeLocks,
            &events,
            &schedulingData,
            &operationStorage,
            &operationsStatisticsOutput,
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

