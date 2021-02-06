#pragma once

#include "public.h"

#include "fair_share_tree_snapshot_impl.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

// Manages profiling data of fair share tree.
class TFairShareTreeProfiler
    : public TRefCounted
{
public:
    TFairShareTreeProfiler(
        const TString& treeId,
        const IInvokerPtr& profilingInvoker);

    // Thread affinity: Control thread.
    NProfiling::TRegistry GetRegistry() const;

    // Thread affinity: Control thread.
    void ProfileOperationUnregistration(const TCompositeSchedulerElement* pool, EOperationState state);

    // Thread affinity: Control thread.
    void RegisterPool(const TCompositeSchedulerElementPtr& element);
    void UnregisterPool(const TCompositeSchedulerElementPtr& element);

    // Thread affinity: Profiler thread.
    void ProfileElements(const TFairShareTreeSnapshotImplPtr& treeSnapshot);

    // Thread affinity: Profiler thread.
    void ApplyJobMetricsDelta(
        const TFairShareTreeSnapshotImplPtr& treeSnapshot,
        const THashMap<TOperationId, TJobMetrics>& jobMetricsPerOperation);

private:
    const NProfiling::TRegistry Registry_;
    const IInvokerPtr ProfilingInvoker_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
     
    struct TUnregisterOperationCounters
    {
        TEnumIndexedVector<EOperationState, NProfiling::TCounter> FinishedCounters;
        NProfiling::TCounter BannedCounter;
    };
    THashMap<TString, TUnregisterOperationCounters> PoolToUnregisterOperationCounters_;


    struct TOperationUserProfilingTag
    {
        TString PoolId;
        TString UserName;
        std::optional<TString> CustomTag;

        bool operator == (const TOperationUserProfilingTag& other) const;
        bool operator != (const TOperationUserProfilingTag& other) const;
    };

    struct TOperationProfilingEntry
    {
        int SlotIndex;
        TString ParentPoolId;
        std::vector<TOperationUserProfilingTag> UserProfilingTags;

        NProfiling::TBufferedProducerPtr BufferedProducer;
    };
    
    struct TPoolProfilingEntry
    {
        TUnregisterOperationCounters UnregisterOperationCounters;

        // We postpone deletion to avoid ABA problem with pool deletion and immediate creation.
        std::optional<TInstant> RemoveTime;

        NProfiling::TBufferedProducerPtr BufferedProducer;
    };

    NProfiling::TGauge PoolCountGauge_;

    THashMap<TString, TJobMetrics> JobMetricsMap_;

    THashMap<TOperationId, TOperationProfilingEntry> OperationIdToProfilingEntry_;
    
    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, PoolNameToProfilingEntryLock_);
    THashMap<TString, TPoolProfilingEntry> PoolNameToProfilingEntry_;

    void RegisterPoolProfiler(const TString& poolName);

    void PrepareOperationProfilingEntries(const TFairShareTreeSnapshotImplPtr& treeSnapshot);

    void CleanupPoolProfilingEntries();

    void ProfileOperations(const TFairShareTreeSnapshotImplPtr& treeSnapshot);
    void ProfilePools(const TFairShareTreeSnapshotImplPtr& treeSnapshot);

    void ProfilePool(
        const TCompositeSchedulerElement* element,
        const TFairShareStrategyTreeConfigPtr& treeConfig,
        bool profilingCompatibilityEnabled,
        const NProfiling::TBufferedProducerPtr& producer);

    void ProfileElement(
        NProfiling::ISensorWriter* writer,
        const TSchedulerElement* element,
        const TFairShareStrategyTreeConfigPtr& treeConfig,
        bool profilingCompatibilityEnabled);
};

DEFINE_REFCOUNTED_TYPE(TFairShareTreeProfiler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
