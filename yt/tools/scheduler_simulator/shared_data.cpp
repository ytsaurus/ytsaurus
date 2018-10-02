#include "shared_data.h"

#include <yt/server/scheduler/fair_share_strategy.h>

namespace NYT {
namespace NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

using namespace NScheduler;
using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TNodeShardEvent::TNodeShardEvent(EEventType type, TInstant time)
    : Type(type)
    , Time(time)
    , OperationId(TGuid())
    , NodeIndex(-1)
    , Job(nullptr)
{ }

TNodeShardEvent TNodeShardEvent::Heartbeat(TInstant time, int nodeIndex, bool scheduledOutOfBand)
{
    TNodeShardEvent event(EEventType::Heartbeat, time);
    event.NodeIndex = nodeIndex;
    event.ScheduledOutOfBand = scheduledOutOfBand;
    return event;
}

TNodeShardEvent TNodeShardEvent::JobFinished(
    TInstant time,
    const TJobPtr& job,
    const TExecNodePtr& execNode,
    int nodeIndex)
{
    TNodeShardEvent event(EEventType::JobFinished, time);
    event.Job = job;
    event.JobNode = execNode;
    event.NodeIndex = nodeIndex;
    return event;
}

bool operator<(const TNodeShardEvent& lhs, const TNodeShardEvent& rhs)
{
    return lhs.Time < rhs.Time;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

THashMap<TOperationId, TOperationDescription> CreateOperationDescriptionByIdMap(
    const std::vector<TOperationDescription>& operations)
{
    THashMap<NScheduler::TOperationId, TOperationDescription> operationDescriptionById;
    for (const auto& operation : operations) {
        operationDescriptionById[operation.Id] = operation;
    }
    return operationDescriptionById;
}

THashMap<TOperationId, TMutable<TOperationStatistics>> CreateOperationsStorage(
    const THashMap<TOperationId, TOperationDescription>& operationDescriptionById)
{
    THashMap<TOperationId, TMutable<TOperationStatistics>> operationStorage;

    for (const auto& pair : operationDescriptionById) {
        auto operationId = pair.first;
        operationStorage.emplace(operationId, TOperationStatistics());
    }

    return operationStorage;
}

} // namespace


TSharedOperationStatistics::TSharedOperationStatistics(const std::vector<TOperationDescription>& operations)
    : OperationDescriptionById_(CreateOperationDescriptionByIdMap(operations))
    , OperationStorage_(CreateOperationsStorage(OperationDescriptionById_))
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

        auto it = OperationDescriptionById_.find(operationId);
        YCHECK(it != OperationDescriptionById_.end());

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
    auto it = OperationDescriptionById_.find(operationId);
    YCHECK(it != OperationDescriptionById_.end());
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

TSharedEventQueue::TSharedEventQueue(
    int heartbeatPeriod,
    TInstant earliestTime,
    int execNodeCount,
    int nodeShardCount,
    TDuration maxAllowedOutrunning)
    : NodeShardEvents_(nodeShardCount)
    , ControlThreadTime_(earliestTime)
    , NodeShardClocks_(nodeShardCount)
    , MaxAllowedOutrunning_(maxAllowedOutrunning)
{
    for (int shardId = 0; shardId < nodeShardCount; ++shardId) {
        NodeShardClocks_[shardId]->store(earliestTime);
    }

    auto heartbeatsStartTime = earliestTime - TDuration::MilliSeconds(heartbeatPeriod);
    for (int nodeIndex = 0; nodeIndex < execNodeCount; ++nodeIndex) {
        const int workerId = nodeIndex % nodeShardCount;
        const auto heartbeatStartDelay = TDuration::MilliSeconds((heartbeatPeriod * nodeIndex) / execNodeCount);
        auto heartbeat = TNodeShardEvent::Heartbeat(heartbeatsStartTime + heartbeatStartDelay, nodeIndex, false);
        NodeShardEvents_[workerId]->insert(heartbeat);
    }
}

void TSharedEventQueue::InsertNodeShardEvent(int workerId, TNodeShardEvent event)
{
    NodeShardEvents_[workerId]->insert(event);
}

TNullable<TNodeShardEvent> TSharedEventQueue::PopNodeShardEvent(int workerId)
{
    auto& localEventsSet = NodeShardEvents_[workerId];
    if (localEventsSet->empty()) {
        return Null;
    }
    auto beginIt = localEventsSet->begin();
    auto event = *beginIt;

    if (event.Time > ControlThreadTime_.load() + MaxAllowedOutrunning_) {
        return Null;
    }

    NodeShardClocks_[workerId]->store(event.Time);
    localEventsSet->erase(beginIt);
    return event;
}

void TSharedEventQueue::WaitForStrugglingNodeShards(TInstant timeBarrier)
{
    for (auto& nodeShardClock : NodeShardClocks_) {
        // Actively waiting.
        while (nodeShardClock->load() < timeBarrier) {
            Yield();
        }
    }
}

void TSharedEventQueue::UpdateControlThreadTime(TInstant time)
{
    ControlThreadTime_.store(time);
}

void TSharedEventQueue::OnNodeShardSimulationFinished(int workerId)
{
    NodeShardClocks_[workerId]->store(TInstant::Max());
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

TSharedSchedulerStrategy::TSharedSchedulerStrategy(
    const ISchedulerStrategyPtr& schedulerStrategy,
    TSchedulerStrategyHost& strategyHost,
    const IInvokerPtr& controlThreadInvoker)
    : SchedulerStrategy_(schedulerStrategy)
    , StrategyHost_(strategyHost)
    , ControlThreadInvoker_(controlThreadInvoker)
{ }

void TSharedSchedulerStrategy::ScheduleJobs(const ISchedulingContextPtr& schedulingContext)
{
    WaitFor(SchedulerStrategy_->ScheduleJobs(schedulingContext))
        .ThrowOnError();
}

void TSharedSchedulerStrategy::PreemptJob(const TJobPtr& job, bool shouldLogEvent)
{
    StrategyHost_.PreemptJob(job, shouldLogEvent);
}

void TSharedSchedulerStrategy::ProcessJobUpdates(
    const std::vector<TJobUpdate>& jobUpdates,
    std::vector<std::pair<TOperationId, TJobId>>* successfullyUpdatedJobs,
    std::vector<TJobId>* jobsToAbort)
{
    int snapshotRevision;
    SchedulerStrategy_->ProcessJobUpdates(jobUpdates, successfullyUpdatedJobs, jobsToAbort, &snapshotRevision);
}

void TSharedSchedulerStrategy::UnregisterOperation(NYT::NScheduler::IOperationStrategyHost* operation)
{
    WaitFor(
        BIND(&ISchedulerStrategy::UnregisterOperation, SchedulerStrategy_, operation)
            .AsyncVia(ControlThreadInvoker_)
            .Run())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSchedulerSimulator
} // namespace NYT
