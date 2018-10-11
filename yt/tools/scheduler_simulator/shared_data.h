#pragma once

#include "private.h"
#include "operation.h"
#include "config.h"
#include "scheduler_strategy_host.h"

#include <yt/server/scheduler/config.h>
#include <yt/server/scheduler/job.h>
#include <yt/server/scheduler/operation.h>

#include <fstream>

namespace NYT {
namespace NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EEventType,
    (Heartbeat)
    (JobFinished)
);

struct TNodeShardEvent
{
    EEventType Type;
    TInstant Time;
    NScheduler::TOperationId OperationId;
    NNodeTrackerClient::TNodeId NodeId;
    NScheduler::TJobPtr Job;
    NScheduler::TExecNodePtr JobNode;
    bool ScheduledOutOfBand;

    static TNodeShardEvent Heartbeat(TInstant time, NNodeTrackerClient::TNodeId nodeId, bool scheduledOutOfBand);

    static TNodeShardEvent JobFinished(
        TInstant time,
        const NScheduler::TJobPtr& job,
        const NScheduler::TExecNodePtr& execNode,
        NNodeTrackerClient::TNodeId nodeId);

private:
    TNodeShardEvent(EEventType type, TInstant time);
};

bool operator<(const TNodeShardEvent& lhs, const TNodeShardEvent& rhs);


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

    TSpinLock Lock;
};

class TSharedOperationStatistics
{
public:
    explicit TSharedOperationStatistics(const std::vector<TOperationDescription>& operations);

    void OnJobStarted(const NScheduler::TOperationId& operationId, TDuration duration);

    void OnJobPreempted(const NScheduler::TOperationId& operationId, TDuration duration);

    void OnJobFinished(const NScheduler::TOperationId& operationId, TDuration duration);

    void OnOperationStarted(const NScheduler::TOperationId& operationId);

    TOperationStatistics OnOperationFinished(
        const NScheduler::TOperationId& operationId,
        TDuration startTime,
        TDuration finishTime);

    const TOperationDescription& GetOperationDescription(const NScheduler::TOperationId& operationId) const;

private:
    const THashMap<NScheduler::TOperationId, TOperationDescription> OperationDescriptionById_;
    const THashMap<NScheduler::TOperationId, TMutable<TOperationStatistics>> OperationStorage_;
};


class TSharedEventQueue
{
public:
    TSharedEventQueue(
        const std::vector<NScheduler::TExecNodePtr>& execNodes,
        int heartbeatPeriod,
        TInstant earliestTime,
        int nodeShardCount,
        TDuration maxAllowedOutrunning);

    void InsertNodeShardEvent(int workerId, TNodeShardEvent event);

    TNullable<TNodeShardEvent> PopNodeShardEvent(int workerId);

    void WaitForStrugglingNodeShards(TInstant timeBarrier);
    void UpdateControlThreadTime(TInstant time);

    void OnNodeShardSimulationFinished(int workerId);

private:
    const std::vector<TMutable<std::multiset<TNodeShardEvent>>> NodeShardEvents_;

    std::atomic<TInstant> ControlThreadTime_;
    const std::vector<TMutable<std::atomic<TInstant>>> NodeShardClocks_;

    const TDuration MaxAllowedOutrunning_;
};


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


class TSharedOperationStatisticsOutput
{
public:
    explicit TSharedOperationStatisticsOutput(const TString& filename);

    void PrintHeader();

    void PrintEntry(const NScheduler::TOperationId& id, const TOperationStatistics& stats);

private:
    std::ofstream OutputStream_;
    TAdaptiveLock Lock_;
};


using TSharedRunningOperationsMap = TLockProtectedMap<NScheduler::TOperationId, NSchedulerSimulator::TOperationPtr>;


class TSharedSchedulerStrategy
{
public:
    TSharedSchedulerStrategy(
        const NScheduler::ISchedulerStrategyPtr& schedulerStrategy,
        TSchedulerStrategyHost& strategyHost,
        const IInvokerPtr& controlThreadInvoker);

    void ScheduleJobs(const NScheduler::ISchedulingContextPtr& schedulingContext);

    void PreemptJob(const NScheduler::TJobPtr& job, bool shouldLogEvent);

    void ProcessJobUpdates(
        const std::vector<NScheduler::TJobUpdate>& jobUpdates,
        std::vector<std::pair<NScheduler::TOperationId, NScheduler::TJobId>>* successfullyUpdatedJobs,
        std::vector<NScheduler::TJobId>* jobsToAbort);

    void UnregisterOperation(NScheduler::IOperationStrategyHost* operation);

private:
    NScheduler::ISchedulerStrategyPtr SchedulerStrategy_;
    TSchedulerStrategyHost& StrategyHost_;
    IInvokerPtr ControlThreadInvoker_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSchedulerSimulator
} // namespace NYT
