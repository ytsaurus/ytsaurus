#pragma once

#include "private.h"
#include "packing.h"
#include "fair_share_tree_element.h"

#include <yt/yt/server/lib/scheduler/config.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TFairShareTreeJobSchedulerOperationSharedState final
{
public:
    TFairShareTreeJobSchedulerOperationSharedState(
        ISchedulerStrategyHost* strategyHost,
        int updatePreemptibleJobsListLoggingPeriod,
        const NLogging::TLogger& logger);

    // Returns resources change.
    TJobResources SetJobResourceUsage(TJobId jobId, const TJobResources& resources);

    TDiskQuota GetTotalDiskQuota() const;

    void PublishFairShare(const TResourceVector& fairShare);

    bool OnJobStarted(
        TSchedulerOperationElement* operationElement,
        TJobId jobId,
        const TJobResourcesWithQuota& resourceUsage,
        const TJobResources& precommittedResources,
        int scheduleJobEpoch,
        bool force = false);
    void OnJobFinished(TSchedulerOperationElement* operationElement, TJobId jobId);
    void UpdatePreemptibleJobsList(const TSchedulerOperationElement* element);

    bool GetPreemptible() const;
    void SetPreemptible(bool value);

    bool IsJobKnown(TJobId jobId) const;

    int GetRunningJobCount() const;
    int GetPreemptibleJobCount() const;
    int GetAggressivelyPreemptibleJobCount() const;

    EJobPreemptionStatus GetJobPreemptionStatus(TJobId jobId) const;
    TJobPreemptionStatusMap GetJobPreemptionStatusMap() const;

    void UpdatePreemptionStatusStatistics(EOperationPreemptionStatus status);
    TPreemptionStatusStatisticsVector GetPreemptionStatusStatistics() const;

    void OnMinNeededResourcesUnsatisfied(
        const ISchedulingContextPtr& schedulingContext,
        const TJobResources& availableResources,
        const TJobResources& minNeededResources);
    TEnumIndexedVector<EJobResourceType, int> GetMinNeededResourcesUnsatisfiedCount();

    void OnOperationDeactivated(const ISchedulingContextPtr& schedulingContext, EDeactivationReason reason);
    TEnumIndexedVector<EDeactivationReason, int> GetDeactivationReasons();
    void ProcessUpdatedStarvationStatus(EStarvationStatus status);
    TEnumIndexedVector<EDeactivationReason, int> GetDeactivationReasonsFromLastNonStarvingTime();

    TInstant GetLastScheduleJobSuccessTime() const;

    TJobResources Disable();
    void Enable();
    bool IsEnabled();

    void RecordPackingHeartbeat(
        const TPackingHeartbeatSnapshot& heartbeatSnapshot,
        const TFairShareStrategyPackingConfigPtr& config);
    bool CheckPacking(
        const TSchedulerOperationElement* operationElement,
        const TPackingHeartbeatSnapshot& heartbeatSnapshot,
        const TJobResourcesWithQuota& jobResources,
        const TJobResources& totalResourceLimits,
        const TFairShareStrategyPackingConfigPtr& config);

private:
    const ISchedulerStrategyHost* StrategyHost_;

    // This value is read and modified only during post update in fair share update invoker.
    EStarvationStatus StarvationStatusAtLastUpdate_ = EStarvationStatus::NonStarving;

    using TJobIdList = std::list<TJobId>;
    TEnumIndexedVector<EJobPreemptionStatus, TJobIdList> JobsPerPreemptionStatus_;

    // NB(eshcherbin): We need to have the most recent fair share during scheduling for correct determination
    // of jobs' preemption statuses. This is why we use this value, which is shared between all snapshots,
    // and keep it updated, instead of using fair share from current snapshot.
    TAtomicObject<TResourceVector> FairShare_;

    std::atomic<bool> Preemptible_ = {true};

    std::atomic<int> RunningJobCount_ = {0};
    TEnumIndexedVector<EJobPreemptionStatus, TJobResources> ResourceUsagePerPreemptionStatus_;

    std::atomic<int> UpdatePreemptibleJobsListCount_ = {0};
    const int UpdatePreemptibleJobsListLoggingPeriod_;

    // TODO(ignat): make it configurable.
    TDuration UpdateStateShardsBackoff_ = TDuration::Seconds(5);

    struct TJobProperties
    {
        //! Determines whether job belongs to the preemptible, aggressively preemptible or non-preemptible jobs list.
        EJobPreemptionStatus PreemptionStatus;

        //! Iterator in the per-operation list pointing to this particular job.
        TJobIdList::iterator JobIdListIterator;

        TJobResources ResourceUsage;

        TDiskQuota DiskQuota;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, JobPropertiesMapLock_);
    THashMap<TJobId, TJobProperties> JobPropertiesMap_;
    TInstant LastScheduleJobSuccessTime_;
    TDiskQuota TotalDiskQuota_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, PreemptionStatusStatisticsLock_);
    TPreemptionStatusStatisticsVector PreemptionStatusStatistics_;

    const NLogging::TLogger Logger;

    //! Thread affinity: control.
    TEnumIndexedVector<EDeactivationReason, int> DeactivationReasons_;
    TEnumIndexedVector<EDeactivationReason, int> DeactivationReasonsFromLastNonStarvingTime_;
    TEnumIndexedVector<EJobResourceType, int> MinNeededResourcesUnsatisfiedCount_;
    TInstant LastDiagnosticCountersUpdateTime_;

    struct alignas(NThreading::CacheLineSize) TStateShard
    {
        TEnumIndexedVector<EDeactivationReason, std::atomic<int>> DeactivationReasons;
        TEnumIndexedVector<EDeactivationReason, std::atomic<int>> DeactivationReasonsFromLastNonStarvingTime;
        TEnumIndexedVector<EJobResourceType, std::atomic<int>> MinNeededResourcesUnsatisfiedCount;
    };
    std::array<TStateShard, MaxNodeShardCount> StateShards_;

    bool Enabled_ = false;

    TPackingStatistics HeartbeatStatistics_;

    void DoUpdatePreemptibleJobsList(const TSchedulerOperationElement* element, int* moveCount);

    void AddJob(TJobId jobId, const TJobResourcesWithQuota& resourceUsage);
    std::optional<TJobResources> RemoveJob(TJobId jobId);

    TJobResources SetJobResourceUsage(TJobProperties* properties, const TJobResources& resources);

    TJobProperties* GetJobProperties(TJobId jobId);
    const TJobProperties* GetJobProperties(TJobId jobId) const;

    // Collect up-to-date values from node shards local counters.
    void UpdateDiagnosticCounters();
};

using TFairShareTreeJobSchedulerOperationSharedStatePtr = TIntrusivePtr<TFairShareTreeJobSchedulerOperationSharedState>;
using TFairShareTreeJobSchedulerSharedOperationStateMap = THashMap<TOperationId, TFairShareTreeJobSchedulerOperationSharedStatePtr>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
