#pragma once

#include "private.h"

#include <yt/yt/ytlib/scheduler/disk_resources.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TStrategyOperationController
    : public TRefCounted
{
public:
    TStrategyOperationController(
        const IOperationStrategyHostPtr& operation,
        const TStrategyOperationControllerConfigPtr& config,
        const std::vector<IInvokerPtr>& nodeShardInvokers);

    void OnScheduleAllocationStarted(const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext);
    void OnScheduleAllocationFinished(const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext);

    TControllerEpoch GetEpoch() const;

    TCompositeNeededResources GetNeededResources() const;
    TAllocationGroupResourcesMap GetGroupedNeededResources() const;
    TAllocationGroupResourcesMap GetInitialGroupedNeededResources() const;
    TJobResources GetAggregatedMinNeededAllocationResources() const;
    TJobResources GetAggregatedInitialMinNeededAllocationResources() const;

    void UpdateGroupedNeededResources();

    void UpdateConcurrentScheduleAllocationThrottlingLimits(const TStrategyOperationControllerConfigPtr& config);
    bool CheckMaxScheduleAllocationCallsOverdraft(int maxScheduleAllocationCalls) const;
    bool IsMaxConcurrentScheduleAllocationCallsPerNodeShardViolated(const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext) const;
    bool IsMaxConcurrentScheduleAllocationExecDurationPerNodeShardViolated(const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext) const;
    bool HasRecentScheduleAllocationFailure(NProfiling::TCpuInstant now) const;
    bool ScheduleAllocationBackoffObserved() const;

    TControllerScheduleAllocationResultPtr ScheduleAllocation(
        const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
        const TJobResources& availableResources,
        const TDiskResources& availableDiskResources,
        TDuration timeLimit,
        const TString& treeId,
        const TString& poolPath,
        std::optional<TDuration> waitingForResourcesOnNodeTimeout);

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

    void UpdateConfig(const TStrategyOperationControllerConfigPtr& config);
    TStrategyOperationControllerConfigPtr GetConfig();

    void SetDetailedLogsEnabled(bool enabled);

private:
    const ISchedulingOperationControllerPtr Controller_;
    const TOperationId OperationId_;

    const NLogging::TLogger Logger;

    NThreading::TReaderWriterSpinLock ConfigLock_;
    TAtomicIntrusivePtr<TStrategyOperationControllerConfig> Config_;

    struct alignas(CacheLineSize) TStateShard
    {
        mutable std::atomic<int> ScheduleAllocationCallsSinceLastUpdate = 0;

        char Padding[CacheLineSize];

        int ConcurrentScheduleAllocationCalls = 0;
        int MaxConcurrentControllerScheduleAllocationCalls = std::numeric_limits<int>::max();
        TDuration ConcurrentScheduleAllocationExecDuration;
        TDuration MaxConcurrentControllerScheduleAllocationExecDuration = TDuration::Max();

        TDuration ScheduleAllocationExecDurationEstimate;
    };
    std::array<TStateShard, MaxNodeShardCount> StateShards_;

    const std::vector<IInvokerPtr> NodeShardInvokers_;

    std::atomic<bool> EnableConcurrentScheduleAllocationExecDurationThrottling_ = false;

    mutable int ScheduleAllocationCallsOverdraft_ = 0;

    std::atomic<NProfiling::TCpuDuration> ScheduleAllocationControllerThrottlingBackoff_;
    std::atomic<NProfiling::TCpuInstant> ScheduleAllocationBackoffDeadline_ = ::Min<NProfiling::TCpuInstant>();
    std::atomic<bool> ScheduleAllocationBackoffObserved_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SaturatedTentativeTreesLock_);
    THashMap<TString, NProfiling::TCpuInstant> TentativeTreeIdToSaturationTime_;

    bool DetailedLogsEnabled_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

DEFINE_REFCOUNTED_TYPE(TStrategyOperationController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
