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
    , public NScheduler::TEventLogHostBase
{
public:
    TSimulatorNodeShard(
        const IInvokerPtr& commonNodeShardInvoker,
        TSchedulerStrategyHost* strategyHost,
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
    TSchedulerStrategyHost* StrategyHost_;
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

    NChunkClient::TMediumDirectoryPtr MediumDirectory_;

    NEventLog::IEventLogWriterPtr RemoteEventLogWriter_;
    std::unique_ptr<NYson::IYsonConsumer> RemoteEventLogConsumer_;

    void Run();
    void RunOnce();

    void OnHeartbeat(const TNodeShardEvent& event);
    void OnJobFinished(const TNodeShardEvent& event);
    void BuildNodeYson(const NScheduler::TExecNodePtr& node, NYTree::TFluentMap fluent);

    void PreemptJob(const NScheduler::TJobPtr& job, bool shouldLogEvent);

    NYson::IYsonConsumer* GetEventLogConsumer() override;

    const NLogging::TLogger* GetEventLogger() override;

    NEventLog::TFluentLogEvent LogFinishedJobFluently(NScheduler::ELogEventType eventType, const NScheduler::TJobPtr& job);
};

int GetNodeShardId(NNodeTrackerClient::TNodeId nodeId, int nodeShardCount);

} // namespace NYT::NSchedulerSimulator
