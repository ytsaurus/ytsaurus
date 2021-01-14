#pragma once

#include "private.h"

#include <yt/core/misc/atomic_object.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategyOperationController
    : public TRefCounted
{
public:
    TFairShareStrategyOperationController(
        IOperationStrategyHost* operation,
        const TFairShareStrategyOperationControllerConfigPtr& config);

    void DecreaseConcurrentScheduleJobCalls(int nodeShardId);
    void IncreaseConcurrentScheduleJobCalls(int nodeShardId);
    void IncreaseScheduleJobCallsSinceLastUpdate(int nodeShardId);

    int GetPendingJobCount() const;
    TJobResources GetNeededResources() const;
    TJobResourcesWithQuotaList GetDetailedMinNeededJobResources() const;
    TJobResources GetAggregatedMinNeededJobResources() const;
    
    void UpdateMinNeededJobResources();

    void CheckMaxScheduleJobCallsOverdraft(int maxScheduleJobCalls, bool* isMaxScheduleJobCallsViolated) const;
    bool IsMaxConcurrentScheduleJobCallsPerNodeShardViolated(
        const ISchedulingContextPtr& schedulingContext,
        int maxConcurrentScheduleJobCallsPerNodeShard) const;
    bool HasRecentScheduleJobFailure(NProfiling::TCpuInstant now) const;

    TControllerScheduleJobResultPtr ScheduleJob(
        const ISchedulingContextPtr& schedulingContext,
        const TJobResources& availableResources,
        TDuration timeLimit,
        const TString& treeId,
        const TFairShareStrategyTreeConfigPtr& treeConfig);

    void AbortJob(
        TJobId jobId,
        EAbortReason abortReason);

    void OnScheduleJobFailed(
        NProfiling::TCpuInstant now,
        const TString& treeId,
        const TControllerScheduleJobResultPtr& scheduleJobResult);

    bool IsSaturatedInTentativeTree(
        NProfiling::TCpuInstant now,
        const TString& treeId,
        TDuration saturationDeactivationTimeout) const;

    void UpdateConfig(const TFairShareStrategyOperationControllerConfigPtr& config);
    TFairShareStrategyOperationControllerConfigPtr GetConfig();

private:
    const IOperationControllerStrategyHostPtr Controller_;
    const TOperationId OperationId_;

    const NLogging::TLogger Logger;

    NConcurrency::TReaderWriterSpinLock ConfigLock_;
    TAtomicObject<TFairShareStrategyOperationControllerConfigPtr> Config_;

    struct TStateShard
    {
        std::atomic<int> ConcurrentScheduleJobCalls = 0;
        mutable std::atomic<int> ScheduleJobCallsSinceLastUpdate = 0;
        char Padding[64];
    };
    std::array<TStateShard, MaxNodeShardCount> StateShards_;

    mutable int ScheduleJobCallsOverdraft_ = 0;

    std::atomic<NProfiling::TCpuDuration> ScheduleJobControllerThrottlingBackoff_;
    std::atomic<NProfiling::TCpuInstant> ScheduleJobBackoffDeadline_ = ::Min<NProfiling::TCpuInstant>();

    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, SaturatedTentativeTreesLock_);
    THashMap<TString, NProfiling::TCpuInstant> TentativeTreeIdToSaturationTime_;
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyOperationController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
