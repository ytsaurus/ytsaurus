#pragma once

#include "private.h"
#include "operation_description.h"
#include "shared_data.h"

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EControlThreadEventType,
    ((FairShareUpdateAndLog) (0))
    ((OperationStarted)      (1))
    ((LogNodes)              (2))
);

struct TControlThreadEvent
{
    EControlThreadEventType Type;
    TInstant Time;
    NScheduler::TOperationId OperationId;

    static TControlThreadEvent OperationStarted(TInstant time, NScheduler::TOperationId id);

    static TControlThreadEvent FairShareUpdateAndLog(TInstant time);

    static TControlThreadEvent LogNodes(TInstant time);

private:
    TControlThreadEvent(EControlThreadEventType type, TInstant time);
};

bool operator<(const TControlThreadEvent& lhs, const TControlThreadEvent& rhs);

////////////////////////////////////////////////////////////////////////////////

class TSimulatorControlThread
    : public NYT::TRefCounted
{
public:
    TSimulatorControlThread(
        const std::vector<NScheduler::TExecNodePtr>* execNodes,
        IOutputStream* eventLogOutputStream,
        IOperationStatisticsOutput* operationStatisticsOutput,
        const TSchedulerSimulatorConfigPtr& config,
        const NScheduler::TSchedulerConfigPtr& schedulerConfig,
        const std::vector<TOperationDescription>& operations,
        TInstant earliestTime);

    void Initialize(const NYson::TYsonString& poolTreesNode);

    bool IsInitialized() const;

    TFuture<void> AsyncRun();

private:
    std::atomic<bool> Initialized_;

    const TDuration FairShareUpdateAndLogPeriod_;
    const TDuration NodesInfoLoggingPeriod_;
    const TSchedulerSimulatorConfigPtr Config_;
    const NConcurrency::TActionQueuePtr ActionQueue_;
    const std::vector<NScheduler::TExecNodePtr>* ExecNodes_;

    std::multiset<TControlThreadEvent> ControlThreadEvents_;

    TSharedEventQueue NodeEventQueue_;
    const NConcurrency::IThreadPoolPtr NodeWorkerThreadPool_;
    std::vector<TSimulatorNodeWorkerPtr> NodeWorkers_;

    std::vector<TSimulatorNodeShardPtr> NodeShards_;
    std::vector<IInvokerPtr> NodeShardInvokers_;

    TSchedulerStrategyHost StrategyHost_;
    NScheduler::ISchedulerStrategyPtr SchedulerStrategy_;
    TSharedSchedulerStrategy SharedSchedulerStrategy_;

    TSharedOperationStatistics OperationStatistics_;
    TSharedRunningOperationsMap RunningOperationsMap_;
    TSharedJobAndOperationCounter JobAndOperationCounter_;

    NLogging::TLogger Logger;

    void Run();
    void RunOnce();

    void OnOperationStarted(const TControlThreadEvent& event);
    void OnFairShareUpdateAndLog(const TControlThreadEvent& event);
    void OnLogNodes(const TControlThreadEvent& event);

    void InsertControlThreadEvent(TControlThreadEvent event);
    TControlThreadEvent PopControlThreadEvent();
};

} // namespace NYT::NSchedulerSimulator
