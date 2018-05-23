#pragma once

#include "private.h"

#include <yt/server/controller_agent/operation_controller.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategyOperationController
    : public TIntrinsicRefCounted
{
public:
    explicit TFairShareStrategyOperationController(IOperationStrategyHost* operation);

    void DecreaseConcurrentScheduleJobCalls();
    void IncreaseConcurrentScheduleJobCalls();

    void SetLastScheduleJobFailTime(NProfiling::TCpuInstant now);

    TJobResourcesWithQuotaList GetDetailedMinNeededJobResources() const;
    TJobResources GetAggregatedMinNeededJobResources() const;
    void UpdateMinNeededJobResources();

    bool IsBlocked(
        NProfiling::TCpuInstant now,
        int maxConcurrentScheduleJobCalls,
        TDuration scheduleJobFailBackoffTime) const;

    NControllerAgent::TScheduleJobResultPtr ScheduleJob(
        const ISchedulingContextPtr& schedulingContext,
        const TJobResources& jobLimits,
        TDuration timeLimit,
        const TString& treeId);

    void AbortJob(
        const TJobId& jobId,
        EAbortReason abortReason);

    int GetPendingJobCount() const;
    TJobResources GetNeededResources() const;

private:
    const IOperationControllerStrategyHostPtr Controller_;
    const TOperationId OperationId_;

    const NLogging::TLogger Logger;

    mutable std::atomic<bool> Blocked_ = {false};
    std::atomic<int> ConcurrentScheduleJobCalls_ = {0};
    std::atomic<NProfiling::TCpuInstant> LastScheduleJobFailTime_ = {0};
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyOperationController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
