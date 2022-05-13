#pragma once

#include "private.h"
#include "packing.h"

#include <yt/yt/server/lib/scheduler/config.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Maybe rename it to TFairShareJobSchedulerOperationState?
class TFairShareTreeJobSchedulerOperationSharedState
    : public TRefCounted
{
public:
    TFairShareTreeJobSchedulerOperationSharedState(
        ISchedulerStrategyHost* strategyHost,
        int updatePreemptableJobsListLoggingPeriod,
        const NLogging::TLogger& logger);

    // Returns resources change.
    TJobResources SetJobResourceUsage(TJobId jobId, const TJobResources& resources);

    TDiskQuota GetTotalDiskQuota() const;

    bool OnJobStarted(
        TSchedulerOperationElement* operationElement,
        TJobId jobId,
        const TJobResourcesWithQuota& resourceUsage,
        const TJobResources& precommittedResources,
        int scheduleJobEpoch,
        bool force = false);
    void OnJobFinished(TSchedulerOperationElement* operationElement, TJobId jobId);
    void UpdatePreemptableJobsList(const TSchedulerOperationElement* element);

    bool GetPreemptable() const;
    void SetPreemptable(bool value);

    bool IsJobKnown(TJobId jobId) const;

    int GetRunningJobCount() const;
    int GetPreemptableJobCount() const;
    int GetAggressivelyPreemptableJobCount() const;

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
    void ResetDeactivationReasonsFromLastNonStarvingTime();
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

    // TODO(eshcherbin): Use TEnumIndexedVector<EJobPreemptionStatus, TJobIdList> here and below.
    using TJobIdList = std::list<TJobId>;
    TJobIdList NonpreemptableJobs_;
    TJobIdList AggressivelyPreemptableJobs_;
    TJobIdList PreemptableJobs_;

    std::atomic<bool> Preemptable_ = {true};

    std::atomic<int> RunningJobCount_ = {0};
    TJobResources TotalResourceUsage_;
    TJobResources NonpreemptableResourceUsage_;
    TJobResources AggressivelyPreemptableResourceUsage_;

    std::atomic<int> UpdatePreemptableJobsListCount_ = {0};
    const int UpdatePreemptableJobsListLoggingPeriod_;

    // TODO(ignat): make it configurable.
    TDuration UpdateStateShardsBackoff_ = TDuration::Seconds(5);

    struct TJobProperties
    {
        //! Determines whether job belongs to the preemptable, aggressively preemptable or non-preemptable jobs list.
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

    struct TStateShard
    {
        TEnumIndexedVector<EDeactivationReason, std::atomic<int>> DeactivationReasons;
        TEnumIndexedVector<EDeactivationReason, std::atomic<int>> DeactivationReasonsFromLastNonStarvingTime;
        TEnumIndexedVector<EJobResourceType, std::atomic<int>> MinNeededResourcesUnsatisfiedCount;
        char Padding1[64];
        TEnumIndexedVector<EDeactivationReason, int> DeactivationReasonsLocal;
        TEnumIndexedVector<EDeactivationReason, int> DeactivationReasonsFromLastNonStarvingTimeLocal;
        TEnumIndexedVector<EJobResourceType, int> MinNeededResourcesUnsatisfiedCountLocal;
        char Padding2[64];
    };
    std::array<TStateShard, MaxNodeShardCount> StateShards_;
    TInstant LastStateShardsUpdateTime_;

    bool Enabled_ = false;

    TPackingStatistics HeartbeatStatistics_;

    void DoUpdatePreemptableJobsList(const TSchedulerOperationElement* element, int* moveCount);

    void AddJob(TJobId jobId, const TJobResourcesWithQuota& resourceUsage);
    std::optional<TJobResources> RemoveJob(TJobId jobId);

    TJobResources SetJobResourceUsage(TJobProperties* properties, const TJobResources& resources);

    TJobProperties* GetJobProperties(TJobId jobId);
    const TJobProperties* GetJobProperties(TJobId jobId) const;

    // Update atomic values from local values in shard state.
    void UpdateShardState();
};

DEFINE_REFCOUNTED_TYPE(TFairShareTreeJobSchedulerOperationSharedState)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
