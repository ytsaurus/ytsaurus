#pragma once

#include "shared_data.h"
#include "config.h"
#include "scheduling_context.h"
#include "control_thread.h"

#include <yt/server/scheduler/public.h>

#include <yt/core/logging/public.h>

#include <yt/core/concurrency/action_queue.h>


namespace NYT::NSchedulerSimulator {

class TSimulatorNodeShard
    : public NYT::TRefCounted
{
public:
    TSimulatorNodeShard(
        const IInvokerPtr& commonNodeShardInvoker,
        TSharedEventQueue* events,
        TSharedSchedulerStrategy* schedulingStrategy,
        TSharedOperationStatistics* operationStatistics,
        IOperationStatisticsOutput* operationStatisticsOutput,
        TSharedRunningOperationsMap* runningOperationsMap,
        TSharedJobAndOperationCounter* jobAndOperationCounter,
        const TSchedulerSimulatorConfigPtr& config,
        const NScheduler::TSchedulerConfigPtr& schedulerConfig,
        TInstant earliestTime,
        int shardId);

    const IInvokerPtr& GetInvoker() const;

    TFuture<void> AsyncRun();

    void RegisterNode(const NScheduler::TExecNodePtr& node);

    void BuildNodesYson(NYTree::TFluentMap fluent);

private:
    THashMap<NNodeTrackerClient::TNodeId, NScheduler::TExecNodePtr> IdToNode_;
    TSharedEventQueue* Events_;
    TSharedSchedulerStrategy* SchedulingStrategy_;
    TSharedOperationStatistics* OperationStatistics_;
    IOperationStatisticsOutput* OperationStatisticsOutput_;
    TSharedRunningOperationsMap* RunningOperationsMap_;
    TSharedJobAndOperationCounter* JobAndOperationCounter_;

    const TSchedulerSimulatorConfigPtr Config_;
    const NScheduler::TSchedulerConfigPtr SchedulerConfig_;
    const TInstant EarliestTime_;
    const int ShardId_;
    const IInvokerPtr Invoker_;

    NLogging::TLogger Logger;

    void Run();
    void RunOnce();

    void OnHeartbeat(const TNodeShardEvent& event);
    void OnJobFinished(const TNodeShardEvent& event);
    void BuildNodeYson(const NScheduler::TExecNodePtr& node, NYTree::TFluentMap fluent);
};

int GetNodeShardId(NNodeTrackerClient::TNodeId nodeId, int nodeShardCount);

} // namespace NYT::NSchedulerSimulator
