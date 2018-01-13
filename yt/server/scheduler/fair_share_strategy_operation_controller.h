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
    DEFINE_BYVAL_RO_PROPERTY(NControllerAgent::IOperationControllerStrategyHostPtr, Controller);

public:
    explicit TFairShareStrategyOperationController(IOperationStrategyHost* operation);

    void DecreaseConcurrentScheduleJobCalls();
    void IncreaseConcurrentScheduleJobCalls();

    void SetLastScheduleJobFailTime(NProfiling::TCpuInstant now);

    void SetMinNeededJobResources(std::vector<TJobResourcesWithQuota> jobResourcesList);
    std::vector<TJobResourcesWithQuota> GetMinNeededJobResourcesList() const;
    TJobResourcesWithQuota GetMinNeededJobResources() const;

    void InvokeMinNeededJobResourcesUpdate();

    bool IsBlocked(
        NProfiling::TCpuInstant now,
        int maxConcurrentScheduleJobCalls,
        TDuration scheduleJobFailBackoffTime) const;

    TScheduleJobResultPtr ScheduleJob(
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
    const TOperationId OperationId_;

    std::atomic<int> ConcurrentScheduleJobCalls_ = {0};
    std::atomic<NProfiling::TCpuInstant> LastScheduleJobFailTime_ = {0};

    NConcurrency::TReaderWriterSpinLock CachedMinNeededJobResourcesLock_;
    std::vector<TJobResourcesWithQuota> CachedMinNeededJobResourcesList_;
    TJobResourcesWithQuota CachedMinNeededJobResources_;
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyOperationController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
