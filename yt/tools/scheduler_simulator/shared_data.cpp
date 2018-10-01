#include "shared_data.h"

#include <yt/server/scheduler/fair_share_strategy.h>

namespace NYT {
namespace NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

using namespace NScheduler;
using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSchedulerEvent::TSchedulerEvent(EEventType type, TInstant time)
    : Type(type)
    , Time(time)
    , OperationId(TGuid())
    , NodeIndex(-1)
    , Job(nullptr)
{ }

TSchedulerEvent TSchedulerEvent::OperationStarted(TInstant time, TOperationId id)
{
    TSchedulerEvent event(EEventType::OperationStarted, time);
    event.OperationId = id;
    return event;
}

TSchedulerEvent TSchedulerEvent::Heartbeat(TInstant time, int nodeIndex, bool scheduledOutOfBand)
{
    TSchedulerEvent event(EEventType::Heartbeat, time);
    event.NodeIndex = nodeIndex;
    event.ScheduledOutOfBand = scheduledOutOfBand;
    return event;
}

TSchedulerEvent TSchedulerEvent::JobFinished(
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

bool operator<(const TSchedulerEvent& lhs, const TSchedulerEvent& rhs)
{
    return lhs.Time < rhs.Time;
}

////////////////////////////////////////////////////////////////////////////////

auto TSharedOperationStatistics::InitializeOperationsStorage(const TOperationDescriptions& operationDescriptions) -> TOperationStorage
{
    TOperationStorage operationStorage;

    for (const auto& operationEntry : operationDescriptions) {
        auto operationId = operationEntry.first;
        operationStorage.emplace(operationId, TOperationStatistics());
    }

    return operationStorage;
}

TSharedOperationStatistics::TSharedOperationStatistics(const TOperationDescriptions& operationDescriptions)
    : OperationDescriptions_(operationDescriptions)
    , OperationStorage_(InitializeOperationsStorage(operationDescriptions))
{ }

void TSharedOperationStatistics::OnJobStarted(const TOperationId& operationId, TDuration duration)
{
    auto& stats = OperationStorage_.at(operationId);
    {
        auto guard = Guard(stats->Lock);
        ++stats->JobCount;
        stats->JobMaxDuration = std::max(stats->JobMaxDuration, duration);
    }
}

void TSharedOperationStatistics::OnJobPreempted(const TOperationId& operationId, TDuration duration)
{
    auto& stats = OperationStorage_.at(operationId);
    {
        auto guard = Guard(stats->Lock);
        --stats->JobCount;
        ++stats->PreemptedJobCount;
        stats->JobsTotalDuration += duration;
        stats->PreemptedJobsTotalDuration += duration;
    }
}

void TSharedOperationStatistics::OnJobFinished(const TOperationId& operationId, TDuration duration)
{
    auto& stats = OperationStorage_.at(operationId);
    {
        auto guard = Guard(stats->Lock);
        stats->JobsTotalDuration += duration;
    }
}

void TSharedOperationStatistics::OnOperationStarted(const TOperationId& operationId)
{
    // Nothing to do.
}

TOperationStatistics TSharedOperationStatistics::OnOperationFinished(
    const TOperationId& operationId,
    TDuration startTime,
    TDuration finishTime)
{
    auto& stats = OperationStorage_.at(operationId);
    {
        auto guard = Guard(stats->Lock);

        stats->StartTime = startTime;
        stats->FinishTime = finishTime;

        auto it = OperationDescriptions_.find(operationId);
        YCHECK(it != OperationDescriptions_.end());

        stats->RealDuration = it->second.Duration;
        stats->OperationType = it->second.Type;
        stats->OperationState = it->second.State;
        stats->InTimeframe = it->second.InTimeframe;

        return *stats;
    }
}

const TOperationDescription& TSharedOperationStatistics::GetOperationDescription(const TOperationId& operationId) const
{
    // No synchronization needed.
    auto it = OperationDescriptions_.find(operationId);
    YCHECK(it != OperationDescriptions_.end());
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

TSharedSchedulerEvents::TSharedSchedulerEvents(
    const std::vector<TOperationDescription>& operations,
    int heartbeatPeriod,
    TInstant earliestTime,
    int execNodeCount,
    int workerCount)
    : LocalEvents_(workerCount)
{
    for (const auto& operation : operations) {
        SharedEvents_.insert(TSchedulerEvent::OperationStarted(operation.StartTime, operation.Id));
    }

    auto heartbeatsStartTime = earliestTime - TDuration::MilliSeconds(heartbeatPeriod);
    for (int nodeIndex = 0; nodeIndex < execNodeCount; ++nodeIndex) {
        const int workerId = nodeIndex % workerCount;
        const auto heartbeatStartDelay = TDuration::MilliSeconds((heartbeatPeriod * nodeIndex) / execNodeCount);
        auto heartbeat = TSchedulerEvent::Heartbeat(heartbeatsStartTime + heartbeatStartDelay, nodeIndex, false);
        LocalEvents_[workerId]->insert(heartbeat);
    }
}

void TSharedSchedulerEvents::InsertEvent(int workerId, TSchedulerEvent event)
{
    YCHECK(event.Type != EEventType::OperationStarted);
    LocalEvents_[workerId]->insert(event);
}

TNullable<TSchedulerEvent> TSharedSchedulerEvents::PopEvent(int workerId)
{
    auto guard = Guard(SharedEventsLock_);
    if (PreferLocalEvent(workerId)) {
        guard.Release();
        return PopLocalEvent(workerId);
    }
    return PopSharedEvent();
}

TNullable<TSchedulerEvent> TSharedSchedulerEvents::PopLocalEvent(int workerId)
{
    auto& localEventsSet = LocalEvents_[workerId];
    if (localEventsSet->empty()) {
        return Null;
    }
    auto beginIt = localEventsSet->begin();
    auto event = *beginIt;
    localEventsSet->erase(beginIt);
    return event;
}

// SharedEventsLock_ must be acquired.
TNullable<TSchedulerEvent> TSharedSchedulerEvents::PopSharedEvent()
{
    if (SharedEvents_.empty()) {
        return Null;
    }
    auto beginIt = SharedEvents_.begin();
    auto event = *beginIt;
    SharedEvents_.erase(beginIt);
    return event;
}

// SharedEventsLock_ must be acquired.
bool TSharedSchedulerEvents::PreferLocalEvent(int workerId)
{
    auto& localEventsSet = LocalEvents_[workerId];
    if (SharedEvents_.empty()) {
        return true;
    }
    if (localEventsSet->empty()) {
        return false;
    }
    return localEventsSet->begin()->Time < SharedEvents_.begin()->Time;
}

////////////////////////////////////////////////////////////////////////////////

TSharedJobAndOperationCounter::TSharedJobAndOperationCounter(int totalOperationCount)
    : RunningJobCount_(0)
    , StartedOperationCount_(0)
    , FinishedOperationCount_(0)
    , TotalOperationCount_(totalOperationCount)
{ }

void TSharedJobAndOperationCounter::OnJobStarted()
{
    ++RunningJobCount_;
}

void TSharedJobAndOperationCounter::OnJobPreempted()
{
    --RunningJobCount_;
}

void TSharedJobAndOperationCounter::OnJobFinished()
{
    --RunningJobCount_;
}

void TSharedJobAndOperationCounter::OnOperationStarted()
{
    ++StartedOperationCount_;
}

void TSharedJobAndOperationCounter::OnOperationFinished()
{
    ++FinishedOperationCount_;
}

int TSharedJobAndOperationCounter::GetRunningJobCount() const
{
    return RunningJobCount_.load();
}

int TSharedJobAndOperationCounter::GetStartedOperationCount() const
{
    return StartedOperationCount_.load();
}

int TSharedJobAndOperationCounter::GetFinishedOperationCount() const
{
    return FinishedOperationCount_.load();
}

int TSharedJobAndOperationCounter::GetTotalOperationCount() const
{
    return TotalOperationCount_;
}

bool TSharedJobAndOperationCounter::HasUnfinishedOperations() const
{
    return FinishedOperationCount_ < TotalOperationCount_;
}

////////////////////////////////////////////////////////////////////////////////

TSharedOperationStatisticsOutput::TSharedOperationStatisticsOutput(const TString& filename)
    : OutputStream_(filename)
{ }

void TSharedOperationStatisticsOutput::PrintHeader()
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

void TSharedOperationStatisticsOutput::PrintEntry(const TOperationId& id, const TOperationStatistics& stats)
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

////////////////////////////////////////////////////////////////////////////////

namespace {

INodePtr LoadPoolTrees(const TString& poolTreesFilename)
{
    try {
        TIFStream configStream(poolTreesFilename);
        return ConvertToNode(&configStream);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading pool trees ") << ex;
    }
}

} // namespace


TSharedSchedulingStrategy::TSharedSchedulingStrategy(
    TSchedulerStrategyHost& strategyHost,
    const IInvokerPtr& invoker,
    const TSchedulerSimulatorConfigPtr& config,
    const TSchedulerConfigPtr& schedulerConfig,
    TInstant earliestTime,
    int workerCount)
    : StrategyHost_(strategyHost)
    , LastFairShareUpdateTime_(earliestTime)
    , FairShareUpdateAndLogPeriod_(schedulerConfig->FairShareUpdatePeriod)
    , MaxAllowedOutrunningPeriod_(FairShareUpdateAndLogPeriod_ + FairShareUpdateAndLogPeriod_)
    , EnableFullEventLog_(config->EnableFullEventLog)
    , WorkerClocks_(workerCount)
{
    for (int workerId = 0; workerId < workerCount; ++workerId) {
        WorkerClocks_[workerId]->store(earliestTime);
    }

    SchedulerStrategy_ = CreateFairShareStrategy(schedulerConfig, &strategyHost, {invoker});
    WaitFor(
        BIND(&ISchedulerStrategy::UpdatePoolTrees, SchedulerStrategy_, LoadPoolTrees(config->PoolTreesFilename))
            .AsyncVia(invoker)
            .Run())
        .ThrowOnError();
}

void TSharedSchedulingStrategy::ScheduleJobs(const ISchedulingContextPtr& schedulingContext)
{
    WaitFor(SchedulerStrategy_->ScheduleJobs(schedulingContext))
        .ThrowOnError();
}

void TSharedSchedulingStrategy::PreemptJob(const TJobPtr& job, bool shouldLogEvent)
{
    StrategyHost_.PreemptJob(job, shouldLogEvent);
}

void TSharedSchedulingStrategy::ProcessJobUpdates(
    const std::vector<TJobUpdate>& jobUpdates,
    std::vector<std::pair<TOperationId, TJobId>>* successfullyUpdatedJobs,
    std::vector<TJobId>* jobsToAbort)
{
    int snapshotRevision;
    SchedulerStrategy_->ProcessJobUpdates(jobUpdates, successfullyUpdatedJobs, jobsToAbort, &snapshotRevision);
}

void TSharedSchedulingStrategy::InitOperationRuntimeParameters(
    const TOperationRuntimeParametersPtr& runtimeParameters,
    const TOperationSpecBasePtr& spec,
    const TString& user,
    EOperationType type)
{
    SchedulerStrategy_->InitOperationRuntimeParameters(runtimeParameters, spec, user, type);
}

void TSharedSchedulingStrategy::OnEvent(int workerId, const TSchedulerEvent& event)
{
    SetWorkerTime(workerId, event.Time);

    auto needToUpdateTree = [&] () {
        return LastFairShareUpdateTime_ + MaxAllowedOutrunningPeriod_ < event.Time;
    };

    while (needToUpdateTree()) {
        auto updateGuard = TTryGuard<TSpinLock>(FairShareUpdateLock_);
        if (updateGuard.WasAcquired()) {
            while (needToUpdateTree()) {
                auto updateTime = LastFairShareUpdateTime_ + FairShareUpdateAndLogPeriod_;
                WaitForOldEventsAt(updateTime);
                DoUpdateAndLogAt(updateTime);
                LastFairShareUpdateTime_ = updateTime;
            }
            break;
        }
    }
}

void TSharedSchedulingStrategy::OnSimulationFinished(int workerId)
{
    SetWorkerTime(workerId, TInstant::Max());
}

// This method is supposed to be called only after the simulation is finished.
void TSharedSchedulingStrategy::OnMasterDisconnected(const IInvokerPtr& invoker)
{
    WaitFor(
        BIND(&ISchedulerStrategy::OnMasterDisconnected, SchedulerStrategy_)
            .AsyncVia(invoker)
            .Run())
        .ThrowOnError();
}

void TSharedSchedulingStrategy::SetWorkerTime(int workerId, TInstant currentWorkerTime)
{
    WorkerClocks_[workerId]->store(currentWorkerTime);
}

void TSharedSchedulingStrategy::WaitForOldEventsAt(TInstant timeBarrier)
{
    for (auto& workerClock : WorkerClocks_) {
        while (workerClock->load() < timeBarrier) {
            // Actively waiting.
        }
    }
}

void TSharedSchedulingStrategy::DoUpdateAndLogAt(TInstant updateTime)
{
    auto strategyGuard = Guard(StrategyLock_);

    SchedulerStrategy_->OnFairShareUpdateAt(updateTime);
    if (EnableFullEventLog_) {
        SchedulerStrategy_->OnFairShareLoggingAt(updateTime);
    } else {
        SchedulerStrategy_->OnFairShareEssentialLoggingAt(updateTime);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSchedulerSimulator
} // namespace NYT
