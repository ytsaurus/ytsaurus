#pragma once

#include "shared_data.h"
#include "config.h"
#include "scheduling_context.h"
#include "control_thread.h"

#include <yt/yt/server/scheduler/public.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

class TSimulatorNodeShard
    : public NYT::TRefCounted
    , public NScheduler::TEventLogHostBase
{
public:
    TSimulatorNodeShard(
        int shardId,
        TSharedEventQueue* events,
        TSchedulerStrategyHost* strategyHost,
        TSharedSchedulerStrategy* schedulingStrategy,
        TSharedOperationStatistics* operationStatistics,
        IOperationStatisticsOutput* operationStatisticsOutput,
        TSharedRunningOperationsMap* runningOperationsMap,
        TSharedJobAndOperationCounter* jobAndOperationCounter,
        TInstant earliestTime,
        TSchedulerSimulatorConfigPtr config,
        NScheduler::TSchedulerConfigPtr schedulerConfig);

    void RegisterNode(const NScheduler::TExecNodePtr& node);
    void BuildNodesYson(NYTree::TFluentMap fluent);

    void OnHeartbeat(const TNodeEvent& event);
    void OnAllocationFinished(const TNodeEvent& event);

    const IInvokerPtr& GetInvoker() const;

    void OnSimulationFinished();

    static int GetNodeShardId(NNodeTrackerClient::TNodeId nodeId, int nodeShardCount);

private:
    const int Id_;
    TSharedEventQueue* const Events_;
    TSchedulerStrategyHost* const StrategyHost_;
    TSharedSchedulerStrategy* const SchedulingStrategy_;
    TSharedOperationStatistics* const OperationStatistics_;
    IOperationStatisticsOutput* const OperationStatisticsOutput_;
    TSharedRunningOperationsMap* const RunningOperationsMap_;
    TSharedJobAndOperationCounter* const JobAndOperationCounter_;
    const TInstant EarliestTime_;

    const TSchedulerSimulatorConfigPtr Config_;
    const NScheduler::TSchedulerConfigPtr SchedulerConfig_;
    const NConcurrency::TActionQueuePtr ActionQueue_;
    const NLogging::TLogger Logger;

    THashMap<NNodeTrackerClient::TNodeId, NScheduler::TExecNodePtr> IdToNode_;

    NChunkClient::TMediumDirectoryPtr MediumDirectory_;

    NEventLog::IEventLogWriterPtr RemoteEventLogWriter_;
    std::unique_ptr<NYson::IYsonConsumer> RemoteEventLogConsumer_;

    void BuildNodeYson(const NScheduler::TExecNodePtr& node, NYTree::TFluentMap fluent) const;
    void PreemptAllocation(const NScheduler::TAllocationPtr& allocation, bool shouldLogEvent);

    NYson::IYsonConsumer* GetEventLogConsumer() override;
    const NLogging::TLogger* GetEventLogger() override;

    NEventLog::TFluentLogEvent LogFinishedAllocationFluently(
        NScheduler::ELogEventType eventType,
        const NScheduler::TAllocationPtr& allocation);
};

DEFINE_REFCOUNTED_TYPE(TSimulatorNodeShard)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
