#pragma once

#include "public.h"
#include "fair_share_tree_element.h"

#include <yt/server/lib/scheduler/job_metrics.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Thread affinity: any
struct IFairShareTreeSnapshot
    : public TIntrinsicRefCounted
{
    virtual TFuture<void> ScheduleJobs(const ISchedulingContextPtr& schedulingContext) = 0;
    virtual void ProcessUpdatedJob(TOperationId operationId, TJobId jobId, const TJobResources& delta) = 0;
    virtual void ProcessFinishedJob(TOperationId operationId, TJobId jobId) = 0;
    virtual bool HasOperation(TOperationId operationId) const = 0;
    virtual void ApplyJobMetricsDelta(TOperationId operationId, const TJobMetrics& jobMetricsDelta) = 0;
    virtual const TSchedulingTagFilter& GetNodesFilter() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IFairShareTreeSnapshot);

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategyOperationState
    : public TIntrinsicRefCounted
{
public:
    using TTreeIdToPoolNameMap = THashMap<TString, TPoolName>;

    DEFINE_BYVAL_RO_PROPERTY(IOperationStrategyHost*, Host);
    DEFINE_BYVAL_RO_PROPERTY(TFairShareStrategyOperationControllerPtr, Controller);
    DEFINE_BYREF_RW_PROPERTY(TTreeIdToPoolNameMap, TreeIdToPoolNameMap);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TString>, ErasedTrees);

public:
    explicit TFairShareStrategyOperationState(IOperationStrategyHost* host)
        : Host_(host)
        , Controller_(New<TFairShareStrategyOperationController>(host))
    { }

    TPoolName GetPoolNameByTreeId(const TString& treeId) const
    {
        auto it = TreeIdToPoolNameMap_.find(treeId);
        YCHECK(it != TreeIdToPoolNameMap_.end());
        return it->second;
    }

    void EraseTree(const TString& treeId)
    {
        ErasedTrees_.push_back(treeId);
        YCHECK(TreeIdToPoolNameMap_.erase(treeId) == 1);
    }
};

using TFairShareStrategyOperationStatePtr = TIntrusivePtr<TFairShareStrategyOperationState>;

////////////////////////////////////////////////////////////////////////////////

struct TPoolsUpdateResult
{
    TError Error;
    bool Updated;
};

NProfiling::TTagIdList GetFailReasonProfilingTags(NControllerAgent::EScheduleJobFailReason reason);

////////////////////////////////////////////////////////////////////////////////

class TFairShareTree
    : public TIntrinsicRefCounted
      , public IFairShareTreeHost
{
public:
    TFairShareTree(
        TFairShareStrategyTreeConfigPtr config,
        TFairShareStrategyOperationControllerConfigPtr controllerConfig,
        ISchedulerStrategyHost* host,
        const std::vector<IInvokerPtr>& feasibleInvokers,
        const TString& treeId);
    IFairShareTreeSnapshotPtr CreateSnapshot();

    TFuture<void> ValidateOperationPoolsCanBeUsed(const IOperationStrategyHost* operation, const TPoolName& poolName);

    void ValidatePoolLimits(const IOperationStrategyHost* operation, const TPoolName& poolName);

    void ValidatePoolLimitsOnPoolChange(const IOperationStrategyHost* operation, const TPoolName& newPoolName);
    void ValidateAllOperationsCountsOnPoolChange(TOperationId operationId, const TPoolName& newPoolName);
    bool RegisterOperation(
        const TFairShareStrategyOperationStatePtr& state,
        const TStrategyOperationSpecPtr& spec,
        const TOperationFairShareTreeRuntimeParametersPtr& runtimeParams);

    void UnregisterOperation(const TFairShareStrategyOperationStatePtr& state);

    void OnOperationRemovedFromPool(
        const TFairShareStrategyOperationStatePtr& state,
        const TCompositeSchedulerElementPtr& parent);

    // Returns true if all pool constraints are satisfied.
    bool OnOperationAddedToPool(
        const TFairShareStrategyOperationStatePtr& state,
        const TOperationElementPtr& operationElement);

    void DisableOperation(const TFairShareStrategyOperationStatePtr& state);

    void EnableOperation(const TFairShareStrategyOperationStatePtr& state);

    TPoolsUpdateResult UpdatePools(const NYTree::INodePtr& poolsNode);

    void ChangeOperationPool(
        TOperationId operationId,
        const TFairShareStrategyOperationStatePtr& state,
        const TPoolName& newPool);

    TError CheckOperationUnschedulable(
        TOperationId operationId,
        TDuration safeTimeout,
        int minScheduleJobCallAttempts,
        THashSet<EDeactivationReason> deactivationReasons);

    void UpdateOperationRuntimeParameters(
        TOperationId operationId,
        const TOperationFairShareTreeRuntimeParametersPtr& runtimeParams);

    void UpdateConfig(const TFairShareStrategyTreeConfigPtr& config);

    void UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config);

    void BuildOperationAttributes(TOperationId operationId, NYTree::TFluentMap fluent);

    void BuildOperationProgress(TOperationId operationId, NYTree::TFluentMap fluent);

    void BuildBriefOperationProgress(TOperationId operationId, NYTree::TFluentMap fluent);

    void BuildUserToEphemeralPoolsInDefaultPool(NYTree::TFluentAny fluent);

    // NB: This function is public for testing purposes.
    TError OnFairShareUpdateAt(TInstant now);

    void ProfileFairShare(NProfiling::TMetricsAccumulator& accumulator) const;

    void ResetTreeIndexes();

    void LogOperationsInfo();

    void LogPoolsInfo();

    // NB: This function is public for testing purposes.
    void OnFairShareLoggingAt(TInstant now);

    // NB: This function is public for testing purposes.
    void OnFairShareEssentialLoggingAt(TInstant now);

    void RegisterJobsFromRevivedOperation(TOperationId operationId, const std::vector<TJobPtr>& jobs);

    void BuildPoolsInformation(NYTree::TFluentMap fluent);

    void BuildStaticPoolsInformation(NYTree::TFluentAny fluent);

    void BuildOrchid(NYTree::TFluentMap fluent);

    void BuildFairShareInfo(NYTree::TFluentMap fluent);

    void BuildEssentialFairShareInfo(NYTree::TFluentMap fluent);

    void ResetState();

    const TSchedulingTagFilter& GetNodesFilter() const;

    TPoolName CreatePoolName(const std::optional<TString>& poolFromSpec, const TString& user);

    bool HasOperation(TOperationId operationId);

    virtual TResourceTree* GetResourceTree() override;

    virtual NProfiling::TAggregateGauge& GetProfilingCounter(const TString& name) override;

    std::vector<TOperationId> RunWaitingOperations();

private:
    TFairShareStrategyTreeConfigPtr Config_;
    TFairShareStrategyOperationControllerConfigPtr ControllerConfig_;

    TResourceTreePtr ResourceTree_;

    ISchedulerStrategyHost* const Host_;

    std::vector<IInvokerPtr> FeasibleInvokers_;

    NYTree::INodePtr LastPoolsNodeUpdate_;
    TError LastPoolsNodeUpdateError_;

    const TString TreeId_;
    const NProfiling::TTagId TreeIdProfilingTag_;

    const NLogging::TLogger Logger;

    using TPoolMap = THashMap<TString, TPoolPtr>;
    TPoolMap Pools_;

    THashMap<TString, NProfiling::TTagId> PoolIdToProfilingTagId_;

    THashMap<TString, THashSet<TString>> UserToEphemeralPoolsInDefaultPool_;

    THashMap<TString, THashSet<int>> PoolToSpareSlotIndices_;
    THashMap<TString, int> PoolToMinUnusedSlotIndex_;

    using TOperationElementPtrByIdMap = THashMap<TOperationId, TOperationElementPtr>;
    TOperationElementPtrByIdMap OperationIdToElement_;

    THashMap<TOperationId, TInstant> OperationIdToActivationTime_;

    std::list<TOperationId> WaitingOperationQueue_;

    NConcurrency::TReaderWriterSpinLock NodeIdToLastPreemptiveSchedulingTimeLock_;
    THashMap<NNodeTrackerClient::TNodeId, NProfiling::TCpuInstant> NodeIdToLastPreemptiveSchedulingTime_;

    std::vector<TSchedulingTagFilter> RegisteredSchedulingTagFilters_;
    std::vector<int> FreeSchedulingTagFilterIndexes_;
    struct TSchedulingTagFilterEntry
    {
        int Index;
        int Count;
    };
    THashMap<TSchedulingTagFilter, TSchedulingTagFilterEntry> SchedulingTagFilterToIndexAndCount_;

    TRootElementPtr RootElement_;

    struct TRootElementSnapshot
        : public TIntrinsicRefCounted
    {
        TRootElementPtr RootElement;
        TOperationElementByIdMap OperationIdToElement;
        TFairShareStrategyTreeConfigPtr Config;

        TOperationElement* FindOperationElement(TOperationId operationId) const;
    };

    typedef TIntrusivePtr<TRootElementSnapshot> TRootElementSnapshotPtr;
    TRootElementSnapshotPtr RootElementSnapshot_;

    class TFairShareTreeSnapshot
        : public IFairShareTreeSnapshot
    {
    public:
        TFairShareTreeSnapshot(TFairShareTreePtr tree, TRootElementSnapshotPtr rootElementSnapshot, const NLogging::TLogger& logger);

        virtual TFuture<void> ScheduleJobs(const ISchedulingContextPtr& schedulingContext) override;

        virtual void ProcessUpdatedJob(TOperationId operationId, TJobId jobId, const TJobResources& delta) override;

        virtual void ProcessFinishedJob(TOperationId operationId, TJobId jobId) override;

        virtual void ApplyJobMetricsDelta(TOperationId operationId, const TJobMetrics& jobMetricsDelta) override;

        virtual bool HasOperation(TOperationId operationId) const override;

        virtual const TSchedulingTagFilter& GetNodesFilter() const override;

    private:
        const TIntrusivePtr<TFairShareTree> Tree_;
        const TRootElementSnapshotPtr RootElementSnapshot_;
        const NLogging::TLogger Logger;
        const TSchedulingTagFilter NodesFilter_;
    };

    TDynamicAttributesList GlobalDynamicAttributes_;

    TFairShareSchedulingStage NonPreemptiveSchedulingStage_;
    TFairShareSchedulingStage PreemptiveSchedulingStage_;
    TFairShareSchedulingStage PackingFallbackSchedulingStage_;

    NProfiling::TAggregateGauge FairShareUpdateTimeCounter_;
    NProfiling::TAggregateGauge FairShareLogTimeCounter_;
    NProfiling::TAggregateGauge AnalyzePreemptableJobsTimeCounter_;

    TSpinLock CustomProfilingCountersLock_;
    THashMap<TString, std::unique_ptr<NProfiling::TAggregateGauge>> CustomProfilingCounters_;

    NProfiling::TCpuInstant LastSchedulingInformationLoggedTime_ = 0;

    TDynamicAttributes GetGlobalDynamicAttributes(const TSchedulerElementPtr& element) const;

    void DoScheduleJobsWithoutPreemptionImpl(
        const TRootElementSnapshotPtr& rootElementSnapshot,
        TFairShareContext* context,
        NProfiling::TCpuInstant startTime,
        bool ignorePacking,
        bool oneJobOnly);
    void DoScheduleJobsWithoutPreemption(
        const TRootElementSnapshotPtr& rootElementSnapshot,
        TFairShareContext* context,
        NProfiling::TCpuInstant startTime);
    void DoScheduleJobsWithPreemption(
        const TRootElementSnapshotPtr& rootElementSnapshot,
        TFairShareContext* context,
        NProfiling::TCpuInstant startTime);
    void DoScheduleJobsPackingFallback(
        const TRootElementSnapshotPtr& rootElementSnapshot,
        TFairShareContext* context,
        NProfiling::TCpuInstant startTime);
    void DoScheduleJobs(
        const ISchedulingContextPtr& schedulingContext,
        const TRootElementSnapshotPtr& rootElementSnapshot);

    void PreemptJob(
        const TJobPtr& job,
        const TOperationElementPtr& operationElement,
        TFairShareContext* context) const;

    const TCompositeSchedulerElement* FindPoolViolatingMaxRunningOperationCount(const TCompositeSchedulerElement* pool);
    const TCompositeSchedulerElement* FindPoolWithViolatedOperationCountLimit(const TCompositeSchedulerElementPtr& element);

    void DoRegisterPool(const TPoolPtr& pool);
    void RegisterPool(const TPoolPtr& pool, const TCompositeSchedulerElementPtr& parent);
    void ReconfigurePool(const TPoolPtr& pool, const TPoolConfigPtr& config);
    void UnregisterPool(const TPoolPtr& pool);

    bool TryAllocatePoolSlotIndex(const TString& poolName, int slotIndex);

    void AllocateOperationSlotIndex(const TFairShareStrategyOperationStatePtr& state, const TString& poolName);
    void ReleaseOperationSlotIndex(const TFairShareStrategyOperationStatePtr& state, const TString& poolName);

    void BuildEssentialOperationProgress(TOperationId operationId, NYTree::TFluentMap fluent);

    int RegisterSchedulingTagFilter(const TSchedulingTagFilter& filter);

    void UnregisterSchedulingTagFilter(int index);
    void UnregisterSchedulingTagFilter(const TSchedulingTagFilter& filter);

    TPoolPtr FindPool(const TString& id);
    TPoolPtr GetPool(const TString& id);

    TPoolPtr GetOrCreatePool(const TPoolName& poolName, TString userName);

    NProfiling::TTagId GetPoolProfilingTag(const TString& id);

    TOperationElementPtr FindOperationElement(TOperationId operationId);
    TOperationElementPtr GetOperationElement(TOperationId operationId);

    TRootElementSnapshotPtr CreateRootElementSnapshot();

    void BuildEssentialPoolsInformation(NYTree::TFluentMap fluent);
    void BuildElementYson(const TSchedulerElementPtr& element, NYTree::TFluentMap fluent);
    void BuildEssentialElementYson(const TSchedulerElementPtr& element, NYTree::TFluentMap fluent, bool shouldPrintResourceUsage);
    void BuildEssentialPoolElementYson(const TSchedulerElementPtr& element, NYTree::TFluentMap fluent);
    void BuildEssentialOperationElementYson(const TSchedulerElementPtr& element, NYTree::TFluentMap fluent);

    NYTree::TYPath GetPoolPath(const TCompositeSchedulerElementPtr& element);
    TCompositeSchedulerElementPtr GetDefaultParentPool();
    TCompositeSchedulerElementPtr GetPoolOrParent(const TPoolName& poolName);

    void ValidateOperationCountLimit(const IOperationStrategyHost* operation, const TPoolName& poolName);
    void ValidateEphemeralPoolLimit(const IOperationStrategyHost* operation, const TPoolName& poolName);
    void DoValidateOperationPoolsCanBeUsed(const IOperationStrategyHost* operation, const TPoolName& poolName);

    void ProfileOperationElement(NProfiling::TMetricsAccumulator& accumulator, TOperationElementPtr element) const;
    void ProfileCompositeSchedulerElement(NProfiling::TMetricsAccumulator& accumulator, TCompositeSchedulerElementPtr element) const;
    void ProfileSchedulerElement(NProfiling::TMetricsAccumulator& accumulator, const TSchedulerElementPtr& element, const TString& profilingPrefix, const NProfiling::TTagIdList& tags) const;
};

DEFINE_REFCOUNTED_TYPE(TFairShareTree)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
