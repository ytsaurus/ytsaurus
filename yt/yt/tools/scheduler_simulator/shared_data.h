#pragma once

#include "private.h"
#include "operation.h"
#include "config.h"
#include "scheduler_strategy_host.h"

#include <yt/yt/server/lib/scheduler/config.h>

#include <yt/yt/server/scheduler/allocation.h>
#include <yt/yt/server/scheduler/operation.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <fstream>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ENodeEventType,
    (Heartbeat)
    (AllocationFinished)
);

struct TNodeEvent
{
    ENodeEventType Type;
    TInstant Time;
    NNodeTrackerClient::TNodeId NodeId;
    NScheduler::TAllocationPtr Allocation;
    NScheduler::TExecNodePtr AllocationNode;
    bool ScheduledOutOfBand = false;
};

bool operator<(const TNodeEvent& lhs, const TNodeEvent& rhs);

////////////////////////////////////////////////////////////////////////////////

TNodeEvent CreateHeartbeatNodeEvent(TInstant time, NNodeTrackerClient::TNodeId nodeId, bool scheduledOutOfBand);

TNodeEvent CreateAllocationFinishedNodeEvent(
    TInstant time,
    const NScheduler::TAllocationPtr& allocation,
    const NScheduler::TExecNodePtr& execNode,
    NNodeTrackerClient::TNodeId nodeId);

////////////////////////////////////////////////////////////////////////////////

struct TOperationStatistics
{
    int JobCount = 0;
    int PreemptedJobCount = 0;
    TDuration JobMaxDuration;
    TDuration JobsTotalDuration;
    TDuration PreemptedJobsTotalDuration;

    // These fields are not accumulative. They are set exactly once when the operation is finished.
    TDuration StartTime;
    TDuration FinishTime;
    TDuration RealDuration;
    NScheduler::EOperationType OperationType;
    TString OperationState;
    bool InTimeframe = false;
};

class TSharedOperationStatistics
{
public:
    explicit TSharedOperationStatistics(std::vector<TOperationDescription> operations);

    void OnJobStarted(NScheduler::TOperationId operationId, TDuration duration);

    void OnJobPreempted(NScheduler::TOperationId operationId, TDuration duration);

    void OnJobFinished(NScheduler::TOperationId operationId, TDuration duration);

    void OnOperationStarted(NScheduler::TOperationId operationId);

    TOperationStatistics OnOperationFinished(
        NScheduler::TOperationId operationId,
        TDuration startTime,
        TDuration finishTime);

    const TOperationDescription& GetOperationDescription(NScheduler::TOperationId operationId) const;

private:
    struct TOperationStatisticsWithLock final
    {
        TOperationStatistics Value;
        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
    };

    using TOperationDescriptionMap = THashMap<NScheduler::TOperationId, TOperationDescription>;
    using TOperationStatisticsMap = THashMap<NScheduler::TOperationId, TIntrusivePtr<TOperationStatisticsWithLock>>;

    static TOperationDescriptionMap CreateOperationDescriptionMap(std::vector<TOperationDescription> operations);
    static TOperationStatisticsMap CreateOperationsStorageMap(const TOperationDescriptionMap& operationDescriptions);

    const TOperationDescriptionMap IdToOperationDescription_;
    const TOperationStatisticsMap IdToOperationStorage_;
};

////////////////////////////////////////////////////////////////////////////////

class TSharedEventQueue
{
public:
    TSharedEventQueue(
        const std::vector<NScheduler::TExecNodePtr>& execNodes,
        int heartbeatPeriod,
        TInstant earliestTime,
        int nodeWorkerCount,
        TDuration maxAllowedOutrunning);

    void InsertNodeEvent(TNodeEvent event);

    std::optional<TNodeEvent> PopNodeEvent(int workerId);

    void WaitForStrugglingNodeWorkers(TInstant timeBarrier);
    void UpdateControlThreadTime(TInstant time);

    void OnNodeWorkerSimulationFinished(int workerId);

private:
    const std::vector<TMutable<std::multiset<TNodeEvent>>> NodeWorkerEvents_;

    std::atomic<TInstant> ControlThreadTime_;
    const std::vector<TMutable<std::atomic<TInstant>>> NodeWorkerClocks_;

    const TDuration MaxAllowedOutrunning_;

    int GetNodeWorkerId(NNodeTrackerClient::TNodeId nodeId) const;
};

////////////////////////////////////////////////////////////////////////////////

class TSharedJobAndOperationCounter
{
public:
    explicit TSharedJobAndOperationCounter(int totalOperationCount);

    void OnJobStarted();

    void OnJobPreempted();

    void OnJobFinished();

    void OnOperationStarted();

    void OnOperationFinished();

    int GetRunningJobCount() const;

    int GetStartedOperationCount() const;

    int GetFinishedOperationCount() const;

    int GetTotalOperationCount() const;

    bool HasUnfinishedOperations() const;

private:
    std::atomic<int> RunningJobCount_;
    std::atomic<int> StartedOperationCount_;
    std::atomic<int> FinishedOperationCount_;
    const int TotalOperationCount_;
};

////////////////////////////////////////////////////////////////////////////////

class IOperationStatisticsOutput
{
public:
    virtual void PrintEntry(NScheduler::TOperationId id, TOperationStatistics stats) = 0;

    virtual ~IOperationStatisticsOutput() = default;

protected:
    IOperationStatisticsOutput() = default;
};

class TSharedOperationStatisticsOutput
    : public IOperationStatisticsOutput
{
public:
    explicit TSharedOperationStatisticsOutput(const TString& filename);

    void PrintEntry(NScheduler::TOperationId id, TOperationStatistics stats) override;

private:
    std::ofstream OutputStream_;
    bool HeaderPrinted_ = false;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
};

using TSharedRunningOperationsMap = TLockProtectedMap<NScheduler::TOperationId, NSchedulerSimulator::TOperationPtr>;

////////////////////////////////////////////////////////////////////////////////

class TSharedSchedulerStrategy
{
public:
    TSharedSchedulerStrategy(
        const NScheduler::ISchedulerStrategyPtr& schedulerStrategy,
        TSchedulerStrategyHost& strategyHost,
        const IInvokerPtr& controlThreadInvoker);

    NScheduler::INodeHeartbeatStrategyProxyPtr CreateNodeHeartbeatStrategyProxy(
        NNodeTrackerClient::TNodeId nodeId,
        const TString& address,
        const TBooleanFormulaTags& tags,
        NScheduler::TMatchingTreeCookie cookie) const;

    void PreemptAllocation(const NScheduler::TAllocationPtr& allocation);

    void ProcessAllocationUpdates(
        const std::vector<NScheduler::TAllocationUpdate>& allocationUpdates,
        THashSet<NScheduler::TAllocationId>* allocationsToPostpone,
        THashMap<NScheduler::TAllocationId, NScheduler::EAbortReason>* allocationsToAbort);

    void UnregisterOperation(NScheduler::IOperationStrategyHost* operation);

    void BuildSchedulingAttributesForNode(
        NNodeTrackerClient::TNodeId nodeId,
        const TString& nodeAddress,
        const TBooleanFormulaTags& nodeTags,
        NYTree::TFluentMap fluent) const;

private:
    NScheduler::ISchedulerStrategyPtr SchedulerStrategy_;
    TSchedulerStrategyHost& StrategyHost_;
    IInvokerPtr ControlThreadInvoker_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
