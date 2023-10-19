#pragma once

#include "private.h"

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategyOperationController
    : public TRefCounted
{
public:
    TFairShareStrategyOperationController(
        IOperationStrategyHost* operation,
        const TFairShareStrategyOperationControllerConfigPtr& config,
        int nodeShardCount);

    void OnScheduleJobStarted(const ISchedulingContextPtr& schedulingContext);
    void OnScheduleJobFinished(const ISchedulingContextPtr& schedulingContext);

    TControllerEpoch GetEpoch() const;

    TCompositeNeededResources GetNeededResources() const;
    TJobResourcesWithQuotaList GetDetailedMinNeededJobResources() const;
    TJobResources GetAggregatedMinNeededJobResources() const;
    TJobResources GetAggregatedInitialMinNeededJobResources() const;

    void UpdateMinNeededJobResources();

    void UpdateConcurrentScheduleJobThrottlingLimits(const TFairShareStrategyOperationControllerConfigPtr& config);
    bool CheckMaxScheduleJobCallsOverdraft(int maxScheduleJobCalls) const;
    bool IsMaxConcurrentScheduleJobCallsPerNodeShardViolated(const ISchedulingContextPtr& schedulingContext) const;
    bool IsMaxConcurrentScheduleJobExecDurationPerNodeShardViolated(const ISchedulingContextPtr& schedulingContext) const;
    bool HasRecentScheduleJobFailure(NProfiling::TCpuInstant now) const;
    bool ScheduleJobBackoffObserved() const;

    TControllerScheduleJobResultPtr ScheduleJob(
        const ISchedulingContextPtr& schedulingContext,
        const TJobResources& availableResources,
        TDuration timeLimit,
        const TString& treeId,
        const TString& poolPath,
        const TFairShareStrategyTreeConfigPtr& treeConfig);

    // TODO(eshcherbin): Move to private.
    void AbortJob(
        TJobId jobId,
        EAbortReason abortReason,
        TControllerEpoch jobEpoch);

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

    void SetDetailedLogsEnabled(bool enabled);

private:
    const IOperationControllerStrategyHostPtr Controller_;
    const TOperationId OperationId_;

    const NLogging::TLogger Logger;

    NThreading::TReaderWriterSpinLock ConfigLock_;
    TAtomicIntrusivePtr<TFairShareStrategyOperationControllerConfig> Config_;

    struct alignas(CacheLineSize) TStateShard
    {
        mutable std::atomic<int> ScheduleJobCallsSinceLastUpdate = 0;

        char Padding[CacheLineSize];

        int ConcurrentScheduleJobCalls = 0;
        TDuration ConcurrentScheduleJobExecDuration;

        TDuration ScheduleJobExecDurationEstimate;
    };
    std::array<TStateShard, MaxNodeShardCount> StateShards_;

    const int NodeShardCount_;

    std::atomic<int> MaxConcurrentControllerScheduleJobCallsPerNodeShard_;
    std::atomic<TDuration> MaxConcurrentControllerScheduleJobExecDurationPerNodeShard_;

    std::atomic<bool> EnableConcurrentScheduleJobExecDurationThrottling_ = false;

    mutable int ScheduleJobCallsOverdraft_ = 0;

    std::atomic<NProfiling::TCpuDuration> ScheduleJobControllerThrottlingBackoff_;
    std::atomic<NProfiling::TCpuInstant> ScheduleJobBackoffDeadline_ = ::Min<NProfiling::TCpuInstant>();
    std::atomic<bool> ScheduleJobBackoffObserved_ = {false};

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SaturatedTentativeTreesLock_);
    THashMap<TString, NProfiling::TCpuInstant> TentativeTreeIdToSaturationTime_;

    bool DetailedLogsEnabled_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyOperationController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
