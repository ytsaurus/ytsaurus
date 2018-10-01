#pragma once

#include "shared_data.h"
#include "config.h"
#include "scheduling_context.h"

#include <yt/server/scheduler/public.h>

#include <yt/core/logging/public.h>

namespace NYT {
namespace NSchedulerSimulator {

class TNodeShard
    : public NYT::TRefCounted
{
public:
    TNodeShard(
        std::vector<NScheduler::TExecNodePtr>* execNodes,
        TSharedSchedulerEvents* events,
        TSharedSchedulingStrategy* schedulingData,
        TSharedOperationStatistics* operationsStatistics,
        TSharedOperationStatisticsOutput* operationStatisticsOutput,
        TSharedRunningOperationsMap* runningOperationsMap,
        TSharedJobAndOperationCounter* jobOperationCounter,
        const TSchedulerSimulatorConfigPtr& config,
        const NScheduler::TSchedulerConfigPtr& schedulerConfig,
        TInstant earliestTime,
        int workerId);

    void Run();

    void RunOnce();

private:
    std::vector<NScheduler::TExecNodePtr>* ExecNodes_;
    TSharedSchedulerEvents* Events_;
    TSharedSchedulingStrategy* SchedulingStrategy_;
    TSharedOperationStatistics* OperationStatistics_;
    TSharedOperationStatisticsOutput* OperationStatisticsOutput_;
    TSharedRunningOperationsMap* RunningOperationsMap_;
    TSharedJobAndOperationCounter* JobAndOperationCounter_;

    const TSchedulerSimulatorConfigPtr Config_;
    const NScheduler::TSchedulerConfigPtr SchedulerConfig_;
    const TInstant EarliestTime_;
    const int WorkerId_;

    NLogging::TLogger Logger;
};

} // namespace NSchedulerSimulator
} // namespace NYT
