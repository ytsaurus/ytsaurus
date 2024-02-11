#pragma once

#include "private.h"

#include <yt/yt/ytlib/scheduler/disk_resources.h>

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

    void OnScheduleAllocationStarted(const ISchedulingContextPtr& schedulingContext);
    void OnScheduleAllocationFinished(const ISchedulingContextPtr& schedulingContext);

    TControllerEpoch GetEpoch() const;

    TCompositeNeededResources GetNeededResources() const;
    TJobResourcesWithQuotaList GetDetailedMinNeededAllocationResources() const;
    TJobResources GetAggregatedMinNeededAllocationResources() const;
    TJobResources GetAggregatedInitialMinNeededAllocationResources() const;

    void UpdateMinNeededAllocationResources();

    void UpdateConcurrentScheduleAllocationThrottlingLimits(const TFairShareStrategyOperationControllerConfigPtr& config);
    bool CheckMaxScheduleAllocationCallsOverdraft(int maxScheduleAllocationCalls) const;
    bool IsMaxConcurrentScheduleAllocationCallsPerNodeShardViolated(const ISchedulingContextPtr& schedulingContext) const;
    bool IsMaxConcurrentScheduleAllocationExecDurationPerNodeShardViolated(const ISchedulingContextPtr& schedulingContext) const;
    bool HasRecentScheduleAllocationFailure(NProfiling::TCpuInstant now) const;
    bool ScheduleAllocationBackoffObserved() const;

    TControllerScheduleAllocationResultPtr ScheduleAllocation(
        const ISchedulingContextPtr& schedulingContext,
        const TJobResources& availableResources,
        const TDiskResources& availableDiskResources,
        TDuration timeLimit,
        const TString& treeId,
        const TString& poolPath,
        const TFairShareStrategyTreeConfigPtr& treeConfig);

    // TODO(eshcherbin): Move to private.
    void AbortAllocation(
        TAllocationId allocationId,
        EAbortReason abortReason,
        TControllerEpoch allocationEpoch);

    void OnScheduleAllocationFailed(
        NProfiling::TCpuInstant now,
        const TString& treeId,
        const TControllerScheduleAllocationResultPtr& scheduleAllocationResult);

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
        mutable std::atomic<int> ScheduleAllocationCallsSinceLastUpdate = 0;

        char Padding[CacheLineSize];

        int ConcurrentScheduleAllocationCalls = 0;
        TDuration ConcurrentScheduleAllocationExecDuration;

        TDuration ScheduleAllocationExecDurationEstimate;
    };
    std::array<TStateShard, MaxNodeShardCount> StateShards_;

    const int NodeShardCount_;

    std::atomic<int> MaxConcurrentControllerScheduleAllocationCallsPerNodeShard_;
    std::atomic<TDuration> MaxConcurrentControllerScheduleAllocationExecDurationPerNodeShard_;

    std::atomic<bool> EnableConcurrentScheduleAllocationExecDurationThrottling_ = false;

    mutable int ScheduleAllocationCallsOverdraft_ = 0;

    std::atomic<NProfiling::TCpuDuration> ScheduleAllocationControllerThrottlingBackoff_;
    std::atomic<NProfiling::TCpuInstant> ScheduleAllocationBackoffDeadline_ = ::Min<NProfiling::TCpuInstant>();
    std::atomic<bool> ScheduleAllocationBackoffObserved_ = {false};

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SaturatedTentativeTreesLock_);
    THashMap<TString, NProfiling::TCpuInstant> TentativeTreeIdToSaturationTime_;

    bool DetailedLogsEnabled_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyOperationController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
