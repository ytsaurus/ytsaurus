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
    (OperationStarted)
);

struct TSchedulerEvent
{
    EEventType Type;
    TInstant Time;
    NScheduler::TOperationId OperationId;
    int NodeIndex;
    NScheduler::TJobPtr Job;
    NScheduler::TExecNodePtr JobNode;
    bool ScheduledOutOfBand;

    static TSchedulerEvent OperationStarted(TInstant time, NScheduler::TOperationId id);

    static TSchedulerEvent Heartbeat(TInstant time, int nodeIndex, bool scheduledOutOfBand);

    static TSchedulerEvent JobFinished(
        TInstant time,
        NScheduler::TJobPtr job,
        NScheduler::TExecNodePtr execNode,
        int nodeIndex);

private:
    TSchedulerEvent(EEventType type, TInstant time);
};

bool operator<(const TSchedulerEvent& lhs, const TSchedulerEvent& rhs);


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

using TOperationDescriptions = THashMap<NScheduler::TOperationId, TOperationDescription>;

class TSharedOperationStatistics
{
public:
    explicit TSharedOperationStatistics(const TOperationDescriptions& operationDescriptions);

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
    using TOperationStorage = THashMap<NScheduler::TOperationId, TMutable<TOperationStatistics>>;

    const TOperationDescriptions& OperationDescriptions_;
    const TOperationStorage OperationStorage_;

    TOperationStorage InitializeOperationsStorage(const TOperationDescriptions& operationDescriptions);
};


class TSharedSchedulerEvents
{
public:
    TSharedSchedulerEvents(
        const std::vector<TOperationDescription>& operations,
        int heartbeatPeriod,
        TInstant earliestTime,
        int execNodeCount,
        int workerCount);

    void InsertEvent(int workerId, TSchedulerEvent event);

    TNullable<TSchedulerEvent> PopEvent(int workerId);

private:
    const std::vector<TMutable<std::multiset<TSchedulerEvent>>> LocalEvents_;

    // Protected by SharedEventsLock_.
    std::multiset<TSchedulerEvent> SharedEvents_;
    TSpinLock SharedEventsLock_;

    TNullable<TSchedulerEvent> PopLocalEvent(int workerId);

    // SharedEventsLock_ must be acquired.
    TNullable<TSchedulerEvent> PopSharedEvent();

    // SharedEventsLock_ must be acquired.
    bool PreferLocalEvent(int workerId);
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


class TSharedSchedulingStrategy
{
public:
    TSharedSchedulingStrategy(
        TSchedulerStrategyHost& strategyHost,
        const IInvokerPtr& invoker,
        const TSchedulerSimulatorConfigPtr& config,
        const NScheduler::TSchedulerConfigPtr& schedulerConfig,
        TInstant earliestTime,
        int workerCount);

    void ScheduleJobs(const NScheduler::ISchedulingContextPtr& schedulingContext);

    void PreemptJob(const NScheduler::TJobPtr& job, bool shouldLogEvent);

    void ProcessJobUpdates(
        const std::vector<NScheduler::TJobUpdate>& jobUpdates,
        std::vector<std::pair<NScheduler::TOperationId, NScheduler::TJobId>>* successfullyUpdatedJobs,
        std::vector<NScheduler::TJobId>* jobsToAbort);

    void InitOperationRuntimeParameters(
        const NScheduler::TOperationRuntimeParametersPtr& runtimeParameters,
        const NScheduler::TOperationSpecBasePtr& spec,
        const TString& user,
        NControllerAgent::EOperationType type);

    void OnEvent(int workerId, const TSchedulerEvent& event);

    void OnSimulationFinished(int workerId);

#define SchedulerStrategyOpProxy(OpName) \
    template <typename... T> auto OpName(T&&... args) \
    { \
        auto strategyGuard = Guard(StrategyLock_); \
        return SchedulerStrategy_->OpName(std::forward<T>(args)...); \
    }

    // These methods are called only when an operation starts or finishes (rare events).
    SchedulerStrategyOpProxy(RegisterOperation)
    SchedulerStrategyOpProxy(EnableOperation)
    SchedulerStrategyOpProxy(UnregisterOperation)

#undef SchedulerStrategyOpProxy

    // This method is supposed to be called only after the simulation is finished.
    void OnMasterDisconnected(const IInvokerPtr& invoker);

private:
    TSchedulerStrategyHost& StrategyHost_;

    // Protected by StrategyLock_.
    NScheduler::ISchedulerStrategyPtr SchedulerStrategy_;
    TSpinLock StrategyLock_;

    // Protected by FairShareUpdateLock_.
    std::atomic<TInstant> LastFairShareUpdateTime_;
    TSpinLock FairShareUpdateLock_;

    const TDuration FairShareUpdateAndLogPeriod_;
    const TDuration MaxAllowedOutrunningPeriod_;
    const bool EnableFullEventLog_;
    const std::vector<TMutable<std::atomic<TInstant>>> WorkerClocks_;

    void SetWorkerTime(int workerId, TInstant currentWorkerTime);

    void WaitForOldEventsAt(TInstant timeBarrier);

    void DoUpdateAndLogAt(TInstant updateTime);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSchedulerSimulator
} // namespace NYT
