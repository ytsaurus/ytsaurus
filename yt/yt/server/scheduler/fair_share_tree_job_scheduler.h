#pragma once

#include "private.h"
#include "fair_share_tree_element.h"

#include <yt/yt/server/lib/scheduler/config.h>

#include <yt/yt/core/misc/atomic_ptr.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TPreemptiveScheduleJobsStage
{
    TScheduleJobsStage* Stage;
    EOperationPreemptionPriority TargetOperationPreemptionPriority = EOperationPreemptionPriority::None;
    EJobPreemptionLevel MinJobPreemptionLevel = EJobPreemptionLevel::Preemptable;
    bool ForcePreemptionAttempt = false;
};

static const int MaxPreemptiveStageCount = 4;
using TPreemptiveScheduleJobsStageList = TCompactVector<TPreemptiveScheduleJobsStage, MaxPreemptiveStageCount>;

////////////////////////////////////////////////////////////////////////////////

class TFairShareTreeSchedulingSnapshot
    : public TRefCounted
{
    DEFINE_BYREF_RO_PROPERTY(THashSet<int>, SsdPriorityPreemptionMedia);
    DEFINE_BYREF_RO_PROPERTY(TCachedJobPreemptionStatuses, CachedJobPreemptionStatuses);
    DEFINE_BYREF_RO_PROPERTY(TTreeSchedulingSegmentsState, SchedulingSegmentsState);

public:
    TFairShareTreeSchedulingSnapshot(
        TSchedulerRootElementPtr rootElement,
        THashSet<int> ssdPriorityPreemptionMedia,
        TCachedJobPreemptionStatuses cachedJobPreemptionStatuses,
        TTreeSchedulingSegmentsState schedulingSegmentsState);

private:
    TSchedulerRootElementPtr RootElement_;
    TAtomicPtr<TDynamicAttributesListSnapshot> DynamicAttributesListSnapshot_;

    TDynamicAttributesListSnapshotPtr GetDynamicAttributesListSnapshot() const;
    void UpdateDynamicAttributesSnapshot(const TResourceUsageSnapshotPtr& resourceUsageSnapshot);

    friend class TFairShareTreeJobScheduler;
};

DEFINE_REFCOUNTED_TYPE(TFairShareTreeSchedulingSnapshot);

////////////////////////////////////////////////////////////////////////////////

struct TJobSchedulerPostUpdateContext
{
    TManageTreeSchedulingSegmentsContext ManageSchedulingSegmentsContext;
};

////////////////////////////////////////////////////////////////////////////////

class TFairShareTreeJobScheduler
    : public TRefCounted
{
public:
    TFairShareTreeJobScheduler(
        TString treeId,
        ISchedulerStrategyHost* strategyHost,
        TFairShareStrategyTreeConfigPtr config,
        TFairShareTreeProfileManagerPtr treeProfiler);

    void ScheduleJobs(const ISchedulingContextPtr& schedulingContext, const TFairShareTreeSnapshotPtr& treeSnapshot);

    void PreemptJobsGracefully(
        const ISchedulingContextPtr& schedulingContext,
        const TFairShareTreeSnapshotPtr& treeSnapshot);

    int RegisterSchedulingTagFilter(const TSchedulingTagFilter& filter);
    void UnregisterSchedulingTagFilter(int index);
    void UnregisterSchedulingTagFilter(const TSchedulingTagFilter& filter);

    TJobSchedulerPostUpdateContext CreatePostUpdateContext(const TJobResources& totalResourceLimits);
    void PostUpdate(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TJobSchedulerPostUpdateContext* postUpdateContext);
    TFairShareTreeSchedulingSnapshotPtr CreateSchedulingSnapshot(TSchedulerRootElementPtr rootElement, TJobSchedulerPostUpdateContext* postUpdateContext);

    void UpdateConfig(TFairShareStrategyTreeConfigPtr config);

    void OnResourceUsageSnapshotUpdate(const TFairShareTreeSnapshotPtr& treeSnapshot, const TResourceUsageSnapshotPtr& resourceUsageSnapshot) const;

private:
    const TString TreeId_;
    const NLogging::TLogger Logger;
    ISchedulerStrategyHost* const StrategyHost_;

    TFairShareStrategyTreeConfigPtr Config_;

    TFairShareTreeProfileManagerPtr TreeProfiler_;

    TEnumIndexedVector<EJobSchedulingStage, TScheduleJobsStage> SchedulingStages_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, RegisteredSchedulingTagFiltersLock_);
    std::vector<TSchedulingTagFilter> RegisteredSchedulingTagFilters_;
    std::vector<int> FreeSchedulingTagFilterIndexes_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, NodeIdToLastPreemptiveSchedulingTimeLock_);
    THashMap<NNodeTrackerClient::TNodeId, TCpuInstant> NodeIdToLastPreemptiveSchedulingTime_;

    struct TSchedulingTagFilterEntry
    {
        int Index;
        int Count;
    };
    THashMap<TSchedulingTagFilter, TSchedulingTagFilterEntry> SchedulingTagFilterToIndexAndCount_;

    NProfiling::TTimeCounter CumulativeScheduleJobsTime_;

    NProfiling::TCounter ScheduleJobsDeadlineReachedCounter_;

    std::atomic<TCpuInstant> LastSchedulingInformationLoggedTime_ = 0;

    TCachedJobPreemptionStatuses CachedJobPreemptionStatuses_;

    std::optional<THashSet<int>> SsdPriorityPreemptionMedia_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void InitSchedulingStages();
    TPreemptiveScheduleJobsStageList BuildPreemptiveSchedulingStageList(TScheduleJobsContext* context);

    void ScheduleJobsWithoutPreemption(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        TScheduleJobsContext* context,
        TCpuInstant startTime);
    void ScheduleJobsPackingFallback(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        TScheduleJobsContext* context,
        TCpuInstant startTime);
    void DoScheduleJobsWithoutPreemption(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        TScheduleJobsContext* context,
        TCpuInstant startTime,
        bool ignorePacking,
        bool oneJobOnly);

    void ReactivateBadPackingOperations(TScheduleJobsContext* context);

    void ScheduleJobsWithPreemption(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        TScheduleJobsContext* context,
        TCpuInstant startTime,
        EOperationPreemptionPriority targetOperationPreemptionPriority,
        EJobPreemptionLevel minJobPreemptionLevel,
        bool forcePreemptionAttempt);
    void AnalyzePreemptableJobs(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        TScheduleJobsContext* context,
        EOperationPreemptionPriority targetOperationPreemptionPriority,
        EJobPreemptionLevel minJobPreemptionLevel,
        std::vector<TJobWithPreemptionInfo>* unconditionallyPreemptableJobs,
        TNonOwningJobSet* forcefullyPreemptableJobs);
    void PreemptJobsAfterScheduling(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        TScheduleJobsContext* context,
        EOperationPreemptionPriority targetOperationPreemptionPriority,
        std::vector<TJobWithPreemptionInfo> preemptableJobs,
        const TNonOwningJobSet& forcefullyPreemptableJobs,
        const TJobPtr& jobStartedUsingPreemption);

    void AbortJobsSinceResourcesOvercommit(
        const ISchedulingContextPtr& schedulingContext,
        const TFairShareTreeSnapshotPtr& treeSnapshot);

    std::vector<TJobWithPreemptionInfo> CollectJobsWithPreemptionInfo(
        const ISchedulingContextPtr& schedulingContext,
        const TFairShareTreeSnapshotPtr& treeSnapshot) const;
    void SortJobsWithPreemptionInfo(std::vector<TJobWithPreemptionInfo>* jobInfos) const;

    void PreemptJob(
        const TJobPtr& job,
        const TSchedulerOperationElementPtr& operationElement,
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const ISchedulingContextPtr& schedulingContext,
        EJobPreemptionReason preemptionReason) const;

    void UpdateSsdPriorityPreemptionMedia();

    void UpdateCachedJobPreemptionStatuses(TFairSharePostUpdateContext* context);
    void ManageSchedulingSegments(TFairSharePostUpdateContext* postUpdateContext, TManageTreeSchedulingSegmentsContext* manageSegmentsContext) const;
};

DEFINE_REFCOUNTED_TYPE(TFairShareTreeJobScheduler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
