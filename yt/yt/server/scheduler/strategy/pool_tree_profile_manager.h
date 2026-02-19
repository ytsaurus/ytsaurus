#pragma once

#include "public.h"
#include "pool_tree.h"
#include "pool_tree_element.h"

#include <yt/yt/server/scheduler/strategy/policy/public.h>

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

// Manages profiling data of a pool tree.
class TPoolTreeProfileManager
    : public TRefCounted
{
public:
    TPoolTreeProfileManager(
        NProfiling::TProfiler profiler,
        bool sparsifyMetrics,
        const IInvokerPtr& profilingInvoker,
        NPolicy::ISchedulingPolicyPtr schedulingPolicy,
        std::vector<TDuration> perPoolStarvationIntervalBounds);

    // Thread affinity: Control thread.
    NProfiling::TProfiler GetProfiler() const;

    // Thread affinity: Control thread.
    void ProfileOperationUnregistration(const TPoolTreeCompositeElement* pool, EOperationState state);

    // Thread affinity: Control thread.
    void RegisterPool(const TPoolTreeCompositeElementPtr& element);
    void UnregisterPool(const TPoolTreeCompositeElementPtr& element);

    // Thread affinity: Profiler thread.
    void ProfileTree(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const THashMap<TOperationId, TAccumulatedResourceDistribution>& operationIdToAccumulatedResourceDistribution);

    // Thread affinity: Profiler thread.
    void ApplyJobMetricsDelta(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const THashMap<TOperationId, TJobMetrics>& allocationMetricsPerOperation);

    // Thread affinity: Profiler thread.
    void ApplyScheduledAndPreemptedResourcesDelta(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const THashMap<std::optional<NPolicy::EAllocationSchedulingStage>, TOperationIdToJobResources>& operationIdWithStageToScheduledAllocationResourcesDeltas,
        const TEnumIndexedArray<NPolicy::EAllocationPreemptionReason, TOperationIdToJobResources>& operationIdWithReasonToPreemptedAllocationResourcesDeltas,
        const TEnumIndexedArray<NPolicy::EAllocationPreemptionReason, TOperationIdToJobResources>& operationIdWithReasonToPreemptedAllocationResourceTimeDeltas,
        const TEnumIndexedArray<NPolicy::EAllocationPreemptionReason, TOperationIdToJobResources>& operationIdWithReasonToImproperlyPreemptedAllocationResourcesDeltas);

    // Thread affinity: Profiler thread.
    void ProfileStarvationIntervals(const TPoolTreeSnapshotPtr& treeSnapshot);
private:
    const NProfiling::TProfiler Profiler_;
    const bool SparsifyMetrics_;
    const IInvokerPtr ProfilingInvoker_;
    const NPolicy::ISchedulingPolicyPtr SchedulingPolicy_;
    const std::vector<TDuration> PerPoolStarvationIntervalBounds_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    struct TUnregisterOperationCounters
    {
        TEnumIndexedArray<EOperationState, NProfiling::TCounter> FinishedCounters;
        NProfiling::TCounter BannedCounter;
    };
    THashMap<TString, TUnregisterOperationCounters> PoolToUnregisterOperationCounters_;

    struct TOperationUserProfilingTag
    {
        TString PoolId;
        std::string UserName;
        std::optional<TString> CustomTag;

        bool operator==(const TOperationUserProfilingTag& other) const = default;
    };

    struct TOperationState
    {
        int SlotIndex;
        TString ParentPoolId;
        std::vector<TOperationUserProfilingTag> UserProfilingTags;

        NProfiling::TBufferedProducerPtr BufferedProducer;

        TAccumulatedResourceDistribution AccumulatedResourceDistribution;

        TJobMetrics JobMetrics;
    };
    THashMap<TOperationId, TOperationState> OperationIdToState_;

    struct TPoolState
    {
        TUnregisterOperationCounters UnregisterOperationCounters;
        TEnumIndexedArray<EStarvationChangeReason, NProfiling::TEventTimer> StarvationIntervalHistograms;

        // We postpone deletion to avoid ABA problem with pool deletion and immediate creation.
        std::optional<TInstant> RemoveTime;

        NProfiling::TBufferedProducerPtr BufferedProducer;
    };
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, PoolNameToStateLock_);
    THashMap<TString, TPoolState> PoolNameToState_;

    // NB(eshcherbin): Ideally pool's job metrics should be embedded in the state,
    // however, we don't want to acquire the lock when updating job metrics.
    THashMap<TString, TJobMetrics> PoolNameToJobMetrics_;

    NProfiling::TGauge NodeCountGauge_;
    NProfiling::TGauge PoolCountGauge_;
    NProfiling::TGauge TotalElementCountGauge_;

    THashMap<std::optional<NPolicy::EAllocationSchedulingStage>, THashMap<TString, TJobResources>> ScheduledResourcesByStageMap_;
    TEnumIndexedArray<NPolicy::EAllocationPreemptionReason, THashMap<TString, TJobResources>> PreemptedResourcesByReasonMap_;
    TEnumIndexedArray<NPolicy::EAllocationPreemptionReason, THashMap<TString, TJobResources>> PreemptedResourceTimesByReasonMap_;
    TEnumIndexedArray<NPolicy::EAllocationPreemptionReason, THashMap<TString, TJobResources>> ImproperlyPreemptedResourcesByReasonMap_;

    NProfiling::TBufferedProducerPtr DistributedResourcesBufferedProducer_;

    void RegisterPoolProfiler(const TString& poolName);

    void PrepareOperationProfilingEntries(const TPoolTreeSnapshotPtr& treeSnapshot);

    void CleanupPoolProfilingEntries();

    void ProfileOperations(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const THashMap<TOperationId, TAccumulatedResourceDistribution>& operationIdToAccumulatedResourceDistribution);
    void ProfilePools(const TPoolTreeSnapshotPtr& treeSnapshot);

    void ProfilePool(
        const TPoolTreeCompositeElement* element,
        const TStrategyTreeConfigPtr& treeConfig,
        const NProfiling::TBufferedProducerPtr& producer);

    void ProfileElement(
        NProfiling::ISensorWriter* writer,
        const TPoolTreeElement* element,
        const TStrategyTreeConfigPtr& treeConfig);

    void ProfileDistributedResources(const TPoolTreeSnapshotPtr& treeSnapshot);
};

DEFINE_REFCOUNTED_TYPE(TPoolTreeProfileManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
