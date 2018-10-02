#pragma once

#include "shared_data.h"
#include "config.h"
#include "scheduling_context.h"
#include "control_thread.h"

#include <yt/server/scheduler/public.h>

#include <yt/core/logging/public.h>

#include <yt/core/concurrency/action_queue.h>


namespace NYT {
namespace NSchedulerSimulator {

class TSimulatorNodeShard
    : public NYT::TRefCounted
{
public:
    TSimulatorNodeShard(
        const std::vector<NScheduler::TExecNodePtr>* execNodes,
        TSharedEventQueue* events,
        TSharedSchedulerStrategy* schedulingStrategy,
        TSharedOperationStatistics* operationStatistics,
        TSharedOperationStatisticsOutput* operationStatisticsOutput,
        TSharedRunningOperationsMap* runningOperationsMap,
        TSharedJobAndOperationCounter* jobAndOperationCounter,
        const TSchedulerSimulatorConfigPtr& config,
        const NScheduler::TSchedulerConfigPtr& schedulerConfig,
        TInstant earliestTime,
        int shardId);

    const IInvokerPtr& GetInvoker() const;

    TFuture<void> AsyncRun();

private:
    const std::vector<NScheduler::TExecNodePtr>* ExecNodes_;
    TSharedEventQueue* Events_;
    TSharedSchedulerStrategy* SchedulingStrategy_;
    TSharedOperationStatistics* OperationStatistics_;
    TSharedOperationStatisticsOutput* OperationStatisticsOutput_;
    TSharedRunningOperationsMap* RunningOperationsMap_;
    TSharedJobAndOperationCounter* JobAndOperationCounter_;

    const TSchedulerSimulatorConfigPtr Config_;
    const NScheduler::TSchedulerConfigPtr SchedulerConfig_;
    const TInstant EarliestTime_;
    const int ShardId_;
    const NConcurrency::TActionQueuePtr ActionQueue_;

    NLogging::TLogger Logger;

    void Run();
    void RunOnce();

    void OnHeartbeat(const TNodeShardEvent& event);
    void OnJobFinished(const TNodeShardEvent& event);
};

} // namespace NSchedulerSimulator
} // namespace NYT
