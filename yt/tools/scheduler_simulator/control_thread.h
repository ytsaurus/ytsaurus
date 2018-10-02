#pragma once

#include "private.h"
#include "operation_description.h"
#include "shared_data.h"

#include <yt/core/concurrency/action_queue.h>


namespace NYT {
namespace NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EControlThreadEventType,
    ((FairShareUpdateAndLog) (0))
    ((OperationStarted)      (1))
);

struct TControlThreadEvent
{
    EControlThreadEventType Type;
    TInstant Time;
    NScheduler::TOperationId OperationId;

    static TControlThreadEvent OperationStarted(TInstant time, NScheduler::TOperationId id);

    static TControlThreadEvent FairShareUpdateAndLog(TInstant time);

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
        const TSchedulerSimulatorConfigPtr& config,
        const NScheduler::TSchedulerConfigPtr& schedulerConfig,
        const std::vector<TOperationDescription>& operations,
        TInstant earliestTime);

    void Init(const NYTree::INodePtr& poolTreesNode);

    bool IsInitialized() const;

    TFuture<void> AsyncRun();

private:
    std::atomic<bool> Initialized_;

    const TDuration FairShareUpdateAndLogPeriod_;
    const TSchedulerSimulatorConfigPtr Config_;

    std::multiset<TControlThreadEvent> ControlThreadEvents_;
    std::vector<TSimulatorNodeShardPtr> NodeShards_;

    const NConcurrency::TActionQueuePtr ActionQueue_;
    TSchedulerStrategyHost StrategyHost_;
    NScheduler::ISchedulerStrategyPtr SchedulerStrategy_;
    TSharedSchedulerStrategy SchedulerStrategyForNodeShards_;
    TSharedEventQueue NodeShardEvents_;

    TSharedOperationStatistics OperationStatistics_;
    TSharedOperationStatisticsOutput OperationStatisticsOutput_;
    TSharedRunningOperationsMap RunningOperationsMap_;
    TSharedJobAndOperationCounter JobAndOperationCounter_;

    NLogging::TLogger Logger;

    void Run();
    void RunOnce();

    void OnOperationStarted(const TControlThreadEvent& event);
    void OnFairShareUpdateAndLog(const TControlThreadEvent& event);

    void InsertControlThreadEvent(TControlThreadEvent event);
    TControlThreadEvent PopControlThreadEvent();
};

} // namespace NSchedulerSimulator
} // namespace NYT
