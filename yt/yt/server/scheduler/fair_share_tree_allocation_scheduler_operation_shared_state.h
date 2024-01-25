#pragma once

#include "private.h"
#include "packing.h"
#include "fair_share_tree_element.h"

#include <yt/yt/server/lib/scheduler/config.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TFairShareTreeAllocationSchedulerOperationSharedState final
{
public:
    TFairShareTreeAllocationSchedulerOperationSharedState(
        ISchedulerStrategyHost* strategyHost,
        int updatePreemptibleAllocationsListLoggingPeriod,
        const NLogging::TLogger& logger);

    // Returns resources change.
    TJobResources SetAllocationResourceUsage(TAllocationId allocationId, const TJobResources& resources);

    TDiskQuota GetTotalDiskQuota() const;

    void PublishFairShare(const TResourceVector& fairShare);

    bool OnAllocationStarted(
        TSchedulerOperationElement* operationElement,
        TAllocationId allocationId,
        const TJobResourcesWithQuota& resourceUsage,
        const TJobResources& precommittedResources,
        TControllerEpoch scheduleAllocationEpoch,
        bool force = false);
    void OnAllocationFinished(TSchedulerOperationElement* operationElement, TAllocationId allocationId);
    void UpdatePreemptibleAllocationsList(const TSchedulerOperationElement* element);

    bool GetPreemptible() const;
    void SetPreemptible(bool value);

    bool IsAllocationKnown(TAllocationId allocationId) const;

    int GetRunningAllocationCount() const;
    int GetPreemptibleAllocationCount() const;
    int GetAggressivelyPreemptibleAllocationCount() const;

    EAllocationPreemptionStatus GetAllocationPreemptionStatus(TAllocationId allocationId) const;
    TAllocationPreemptionStatusMap GetAllocationPreemptionStatusMap() const;

    void UpdatePreemptionStatusStatistics(EOperationPreemptionStatus status);
    TPreemptionStatusStatisticsVector GetPreemptionStatusStatistics() const;

    void OnMinNeededResourcesUnsatisfied(
        const ISchedulingContextPtr& schedulingContext,
        const TJobResources& availableResources,
        const TJobResources& minNeededResources);
    TEnumIndexedArray<EJobResourceType, int> GetMinNeededResourcesUnsatisfiedCount();

    void IncrementOperationScheduleAllocationAttemptCount(const ISchedulingContextPtr& schedulingContext);
    int GetOperationScheduleAllocationAttemptCount();

    void OnOperationDeactivated(const ISchedulingContextPtr& schedulingContext, EDeactivationReason reason);
    TEnumIndexedArray<EDeactivationReason, int> GetDeactivationReasons();
    void ProcessUpdatedStarvationStatus(EStarvationStatus status);
    TEnumIndexedArray<EDeactivationReason, int> GetDeactivationReasonsFromLastNonStarvingTime();

    TInstant GetLastScheduleAllocationSuccessTime() const;

    TJobResources Disable();
    void Enable();
    bool IsEnabled();

    void RecordPackingHeartbeat(
        const TPackingHeartbeatSnapshot& heartbeatSnapshot,
        const TFairShareStrategyPackingConfigPtr& config);
    bool CheckPacking(
        const TSchedulerOperationElement* operationElement,
        const TPackingHeartbeatSnapshot& heartbeatSnapshot,
        const TJobResourcesWithQuota& allocationResources,
        const TJobResources& totalResourceLimits,
        const TFairShareStrategyPackingConfigPtr& config);

private:
    const ISchedulerStrategyHost* StrategyHost_;

    // This value is read and modified only during post update in fair share update invoker.
    EStarvationStatus StarvationStatusAtLastUpdate_ = EStarvationStatus::NonStarving;

    using TAllocationIdList = std::list<TAllocationId>;
    TEnumIndexedArray<EAllocationPreemptionStatus, TAllocationIdList> AllocationsPerPreemptionStatus_;

    // NB(eshcherbin): We need to have the most recent fair share during scheduling for correct determination
    // of allocations' preemption statuses. This is why we use this value, which is shared between all snapshots,
    // and keep it updated, instead of using fair share from current snapshot.
    TAtomicObject<TResourceVector> FairShare_;

    std::atomic<bool> Preemptible_ = {true};

    std::atomic<int> RunningAllocationCount_ = {0};
    TEnumIndexedArray<EAllocationPreemptionStatus, TJobResources> ResourceUsagePerPreemptionStatus_;

    std::atomic<int> UpdatePreemptibleAllocationsListCount_ = {0};
    const int UpdatePreemptibleAllocationsListLoggingPeriod_;

    // TODO(ignat): make it configurable.
    TDuration UpdateStateShardsBackoff_ = TDuration::Seconds(5);

    struct TAllocationProperties
    {
        //! Determines whether allocation belongs to the preemptible, aggressively preemptible or non-preemptible allocations list.
        EAllocationPreemptionStatus PreemptionStatus;

        //! Iterator in the per-operation list pointing to this particular allocation.
        TAllocationIdList::iterator AllocationIdListIterator;

        TJobResources ResourceUsage;

        TDiskQuota DiskQuota;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, AllocationPropertiesMapLock_);
    THashMap<TAllocationId, TAllocationProperties> AllocationPropertiesMap_;
    TInstant LastScheduleAllocationSuccessTime_;
    TDiskQuota TotalDiskQuota_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, PreemptionStatusStatisticsLock_);
    TPreemptionStatusStatisticsVector PreemptionStatusStatistics_;

    const NLogging::TLogger Logger;

    //! Thread affinity: control.
    TEnumIndexedArray<EDeactivationReason, int> DeactivationReasons_;
    TEnumIndexedArray<EDeactivationReason, int> DeactivationReasonsFromLastNonStarvingTime_;
    TEnumIndexedArray<EJobResourceType, int> MinNeededResourcesUnsatisfiedCount_;
    TInstant LastDiagnosticCountersUpdateTime_;

    //! Thread affinity: control, profiling.
    std::atomic<i64> ScheduleAllocationAttemptCount_;

    struct alignas(CacheLineSize) TStateShard
    {
        TEnumIndexedArray<EDeactivationReason, std::atomic<int>> DeactivationReasons;
        TEnumIndexedArray<EDeactivationReason, std::atomic<int>> DeactivationReasonsFromLastNonStarvingTime;
        TEnumIndexedArray<EJobResourceType, std::atomic<int>> MinNeededResourcesUnsatisfiedCount;

        std::atomic<i64> ScheduleAllocationAttemptCount;
    };
    std::array<TStateShard, MaxNodeShardCount> StateShards_;

    bool Enabled_ = false;

    TPackingStatistics HeartbeatStatistics_;

    void DoUpdatePreemptibleAllocationsList(const TSchedulerOperationElement* element, int* moveCount);

    void AddAllocation(TAllocationId allocationId, const TJobResourcesWithQuota& resourceUsage);
    std::optional<TJobResources> RemoveAllocation(TAllocationId allocationId);

    TJobResources SetAllocationResourceUsage(TAllocationProperties* properties, const TJobResources& resources);

    TAllocationProperties* GetAllocationProperties(TAllocationId allocationId);
    const TAllocationProperties* GetAllocationProperties(TAllocationId allocationId) const;

    // Collect up-to-date values from node shards local counters.
    void UpdateDiagnosticCounters();
};

using TFairShareTreeAllocationSchedulerOperationSharedStatePtr = TIntrusivePtr<TFairShareTreeAllocationSchedulerOperationSharedState>;
using TFairShareTreeAllocationSchedulerSharedOperationStateMap = THashMap<TOperationId, TFairShareTreeAllocationSchedulerOperationSharedStatePtr>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
