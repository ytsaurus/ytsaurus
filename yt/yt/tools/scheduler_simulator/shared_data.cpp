#include "shared_data.h"

#include "node_shard.h"

#include <yt/yt/server/scheduler/fair_share_strategy.h>

#include <random>


namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

using namespace NScheduler;
using namespace NConcurrency;
using namespace NYTree;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

bool operator<(const TNodeEvent& lhs, const TNodeEvent& rhs)
{
    return lhs.Time < rhs.Time;
}

////////////////////////////////////////////////////////////////////////////////

TNodeEvent CreateHeartbeatNodeEvent(TInstant time, TNodeId nodeId, bool scheduledOutOfBand)
{
    return TNodeEvent{
        .Type = ENodeEventType::Heartbeat,
        .Time = time,
        .NodeId = nodeId,
        .ScheduledOutOfBand = scheduledOutOfBand,
    };
}

TNodeEvent CreateAllocationFinishedNodeEvent(TInstant time, const TAllocationPtr& allocation, const TExecNodePtr& execNode, TNodeId nodeId)
{
    return TNodeEvent{
        .Type = ENodeEventType::AllocationFinished,
        .Time = time,
        .NodeId = nodeId,
        .Allocation = allocation,
        .AllocationNode = execNode,
    };
}

////////////////////////////////////////////////////////////////////////////////

TSharedOperationStatistics::TSharedOperationStatistics(std::vector<TOperationDescription> operations)
    : IdToOperationDescription_(CreateOperationDescriptionMap(std::move(operations)))
    , IdToOperationStorage_(CreateOperationsStorageMap(IdToOperationDescription_))
{ }

void TSharedOperationStatistics::OnJobStarted(TOperationId operationId, TDuration duration)
{
    auto& [stats, lock] = *GetOrCrash(IdToOperationStorage_, operationId);
    auto guard = Guard(lock);
    ++stats.JobCount;
    stats.JobMaxDuration = std::max(stats.JobMaxDuration, duration);
}

void TSharedOperationStatistics::OnJobPreempted(TOperationId operationId, TDuration duration)
{
    auto& [stats, lock] = *GetOrCrash(IdToOperationStorage_, operationId);
    auto guard = Guard(lock);
    --stats.JobCount;
    ++stats.PreemptedJobCount;
    stats.JobsTotalDuration += duration;
    stats.PreemptedJobsTotalDuration += duration;
}

void TSharedOperationStatistics::OnJobFinished(TOperationId operationId, TDuration duration)
{
    auto& [stats, lock] = *GetOrCrash(IdToOperationStorage_, operationId);
    auto guard = Guard(lock);
    stats.JobsTotalDuration += duration;
}

void TSharedOperationStatistics::OnOperationStarted(TOperationId /*operationId*/)
{ }

TOperationStatistics TSharedOperationStatistics::OnOperationFinished(
    TOperationId operationId,
    TDuration startTime,
    TDuration finishTime)
{
    auto& [stats, lock] = *GetOrCrash(IdToOperationStorage_, operationId);
    auto guard = Guard(lock);
    stats.StartTime = startTime;
    stats.FinishTime = finishTime;

    const auto& operationDescription = GetOrCrash(IdToOperationDescription_, operationId);

    stats.RealDuration = operationDescription.Duration;
    stats.OperationType = operationDescription.Type;
    stats.OperationState = operationDescription.State;
    stats.InTimeframe = operationDescription.InTimeframe;

    return std::move(stats);
}

const TOperationDescription& TSharedOperationStatistics::GetOperationDescription(TOperationId operationId) const
{
    // No synchronization needed.
    return GetOrCrash(IdToOperationDescription_, operationId);
}

auto TSharedOperationStatistics::CreateOperationDescriptionMap(
    std::vector<TOperationDescription> operations) -> TOperationDescriptionMap
{
    TOperationDescriptionMap operationDescriptionById;
    for (auto&& operation : operations) {
        auto operationId = operation.Id;
        EmplaceOrCrash(operationDescriptionById, operationId, std::move(operation));
    }
    return operationDescriptionById;
}

auto TSharedOperationStatistics::CreateOperationsStorageMap(
    const TOperationDescriptionMap& operationDescriptionById) -> TOperationStatisticsMap
{
    TOperationStatisticsMap operationStorage;
    for (const auto& [operationId, _] : operationDescriptionById) {
        EmplaceOrCrash(operationStorage, operationId, New<TOperationStatisticsWithLock>());
    }
    return operationStorage;
}

////////////////////////////////////////////////////////////////////////////////

TSharedEventQueue::TSharedEventQueue(
    const std::vector<TExecNodePtr>& execNodes,
    int heartbeatPeriod,
    TInstant earliestTime,
    int nodeWorkerCount,
    TDuration maxAllowedOutrunning)
    : NodeWorkerEvents_(nodeWorkerCount)
    , ControlThreadTime_(earliestTime)
    , NodeWorkerClocks_(nodeWorkerCount)
    , MaxAllowedOutrunning_(maxAllowedOutrunning)
{
    for (int shardId = 0; shardId < nodeWorkerCount; ++shardId) {
        NodeWorkerClocks_[shardId]->store(earliestTime);
    }

    auto heartbeatStartTime = earliestTime - TDuration::MilliSeconds(heartbeatPeriod);
    std::mt19937 randomGenerator;
    std::uniform_int_distribution<int> distribution(0, heartbeatPeriod - 1);

    for (const auto& execNode : execNodes) {
        auto heartbeatStartDelay = TDuration::MilliSeconds(distribution(randomGenerator));
        InsertNodeEvent(CreateHeartbeatNodeEvent(
            heartbeatStartTime + heartbeatStartDelay,
            execNode->GetId(),
            /*scheduledOutOfBand*/ false));
    }
}

void TSharedEventQueue::InsertNodeEvent(TNodeEvent event)
{
    int workerId = GetNodeWorkerId(event.NodeId);
    NodeWorkerEvents_[workerId]->insert(std::move(event));
}

std::optional<TNodeEvent> TSharedEventQueue::PopNodeEvent(int workerId)
{
    auto& localEventsSet = NodeWorkerEvents_[workerId];
    if (localEventsSet->empty()) {
        NodeWorkerClocks_[workerId]->store(ControlThreadTime_.load() + MaxAllowedOutrunning_);
        return std::nullopt;
    }
    auto beginIt = localEventsSet->begin();
    auto event = *beginIt;

    NodeWorkerClocks_[workerId]->store(event.Time);
    if (event.Time > ControlThreadTime_.load() + MaxAllowedOutrunning_) {
        return std::nullopt;
    }

    localEventsSet->erase(beginIt);
    return event;
}

void TSharedEventQueue::WaitForStrugglingNodeWorkers(TInstant timeBarrier)
{
    for (auto& nodeWorkerClock : NodeWorkerClocks_) {
        // Actively waiting.
        while (nodeWorkerClock->load() < timeBarrier) {
            Yield();
        }
    }
}

void TSharedEventQueue::UpdateControlThreadTime(TInstant time)
{
    ControlThreadTime_.store(time);
}

void TSharedEventQueue::OnNodeWorkerSimulationFinished(int workerId)
{
    NodeWorkerClocks_[workerId]->store(TInstant::Max());
}

int TSharedEventQueue::GetNodeWorkerId(TNodeId nodeId) const
{
    return THash<TNodeId>()(nodeId) % std::ssize(NodeWorkerEvents_);
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

void TSharedOperationStatisticsOutput::PrintEntry(TOperationId id, TOperationStatistics stats)
{
    auto outputGuard = Guard(Lock_);

    if (!HeaderPrinted_) {
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

        HeaderPrinted_ = true;
    }

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

INodeHeartbeatStrategyProxyPtr TSharedSchedulerStrategy::CreateNodeHeartbeatStrategyProxy(
    TNodeId nodeId,
    const TString& address,
    const TBooleanFormulaTags& tags,
    TMatchingTreeCookie cookie) const
{
    return SchedulerStrategy_->CreateNodeHeartbeatStrategyProxy(nodeId, address, tags, cookie);
}

void TSharedSchedulerStrategy::PreemptAllocation(const TAllocationPtr& allocation)
{
    StrategyHost_.PreemptAllocation(allocation, TDuration::Zero());
}

void TSharedSchedulerStrategy::ProcessAllocationUpdates(
    const std::vector<TAllocationUpdate>& allocationUpdates,
    THashSet<TAllocationId>* allocationsToPostpone,
    THashMap<TAllocationId, EAbortReason>* allocationsToAbort)
{
    SchedulerStrategy_->ProcessAllocationUpdates(allocationUpdates, allocationsToPostpone, allocationsToAbort);
}

void TSharedSchedulerStrategy::UnregisterOperation(NYT::NScheduler::IOperationStrategyHost* operation)
{
    WaitFor(
        BIND(&ISchedulerStrategy::UnregisterOperation, SchedulerStrategy_, operation)
            .AsyncVia(ControlThreadInvoker_)
            .Run())
        .ThrowOnError();
}

void TSharedSchedulerStrategy::BuildSchedulingAttributesForNode(
    TNodeId nodeId,
    const TString& nodeAddress,
    const TBooleanFormulaTags& nodeTags,
    TFluentMap fluent) const
{
    return SchedulerStrategy_->BuildSchedulingAttributesForNode(nodeId, nodeAddress, nodeTags, fluent);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
