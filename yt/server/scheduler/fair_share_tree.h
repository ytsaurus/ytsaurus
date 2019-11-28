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
    virtual void PreemptJobsGracefully(const ISchedulingContextPtr& schedulingContext) = 0;
    virtual void ProcessUpdatedJob(TOperationId operationId, TJobId jobId, const TJobResources& delta) = 0;
    virtual void ProcessFinishedJob(TOperationId operationId, TJobId jobId) = 0;
    virtual bool HasOperation(TOperationId operationId) const = 0;
    virtual bool IsOperationDisabled(TOperationId operationId) const = 0;
    virtual void ApplyJobMetricsDelta(TOperationId operationId, const TJobMetrics& jobMetricsDelta) = 0;
    virtual void ProfileFairShare() const = 0;
    virtual const TSchedulingTagFilter& GetNodesFilter() const = 0;
    virtual TJobResources GetTotalResourceLimits() const = 0;
    virtual std::optional<TSchedulerElementStateSnapshot> GetMaybeStateSnapshotForPool(const TString& poolId) const = 0;
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
    DEFINE_BYVAL_RW_PROPERTY(bool, Enabled);

public:
    explicit TFairShareStrategyOperationState(IOperationStrategyHost* host);

    TPoolName GetPoolNameByTreeId(const TString& treeId) const;

    void EraseTree(const TString& treeId);
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyOperationState)

THashMap<TString, TPoolName> GetOperationPools(const TOperationRuntimeParametersPtr& runtimeParameters);

TFairShareStrategyOperationStatePtr
CreateFairShareStrategyOperationState(IOperationStrategyHost* host);

////////////////////////////////////////////////////////////////////////////////

struct TPoolsUpdateResult
{
    TError Error;
    bool Updated;
};

NProfiling::TTagIdList GetFailReasonProfilingTags(NControllerAgent::EScheduleJobFailReason reason);

////////////////////////////////////////////////////////////////////////////////

//! This class represents fair share tree.
//!
//! We maintain following entities:
//!
//!   * Actual tree, it contains the latest and consistent stucture of pools and operations.
//!     This tree represented by fields #RootElement_, #OperationIdToElement_, #Pools_.
//!     Update of this tree performed in sequentual manner from #Control thread.
//!
//!   * Snapshot of the tree with scheduling attributes (fair share ratios, best leaf descendants et. c).
//!     It is built repeatedly from actual tree by taking snapshot and calculating scheduling attributes.
//!     Clones of this tree are used in heartbeats for scheduling. Also, element attributes from this tree
//!     are used in orchid and for profiling.
//!     This tree represented by fields #GlobalDynamicAttributes_, #ElementIndexes_, #RootElementSnapshot_.
//!     NB: elements of this tree may be invalidated by #Alive flag in resource tree. In this case element cannot be safely used
//!     (corresponding operation or pool can be already deleted from all other scheduler structures).
//!
//!   * Resource tree, it is thread safe tree that maintain shared attributes of tree elements.
//!     More details can be find at #TResourceTree.
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

    TFuture<void> ValidateOperationPoolsCanBeUsed(const IOperationStrategyHost* operation, const TPoolName& poolName);

    void ValidatePoolLimits(const IOperationStrategyHost* operation, const TPoolName& poolName);

    void ValidatePoolLimitsOnPoolChange(const IOperationStrategyHost* operation, const TPoolName& newPoolName);
    void ValidateAllOperationsCountsOnPoolChange(TOperationId operationId, const TPoolName& newPoolName);
    bool RegisterOperation(
        const TFairShareStrategyOperationStatePtr& state,
        const TStrategyOperationSpecPtr& spec,
        const TOperationFairShareTreeRuntimeParametersPtr& runtimeParameters);

    void UnregisterOperation(const TFairShareStrategyOperationStatePtr& state);

    void OnOperationRemovedFromPool(
        const TFairShareStrategyOperationStatePtr& state,
        const TOperationElementPtr& element,
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
        const TOperationFairShareTreeRuntimeParametersPtr& runtimeParameters);

    void UpdateConfig(const TFairShareStrategyTreeConfigPtr& config);

    void UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config);

    void BuildOperationAttributes(TOperationId operationId, NYTree::TFluentMap fluent);

    void BuildOperationProgress(TOperationId operationId, NYTree::TFluentMap fluent);

    void BuildBriefOperationProgress(TOperationId operationId, NYTree::TFluentMap fluent);

    void BuildUserToEphemeralPoolsInDefaultPool(NYTree::TFluentAny fluent);

    void LogOperationsInfo();

    void LogPoolsInfo();

    // NB: This function is public for scheduler simulator.
    TFuture<std::pair<IFairShareTreeSnapshotPtr, TError>> OnFairShareUpdateAt(TInstant now);

    // NB: This function is public for scheduler simulator.
    void OnFairShareLoggingAt(TInstant now);

    // NB: This function is public for scheduler simulator.
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
    bool HasRunningOperation(TOperationId operationId);

    virtual TResourceTree* GetResourceTree() override;

    virtual NProfiling::TAggregateGauge& GetProfilingCounter(const TString& name) override;

    void RunWaitingOperations(TCompositeSchedulerElement* pool);

    std::vector<TOperationId> TryRunAllWaitingOperations();

    std::vector<TOperationId> ExtractActivatableOperations();

    void OnTreeRemoveStarted();

    bool IsBeingRemoved();

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

    TPoolMap Pools_;

    THashMap<TString, NProfiling::TTagId> PoolIdToProfilingTagId_;

    THashMap<TString, THashSet<TString>> UserToEphemeralPoolsInDefaultPool_;

    THashMap<TString, THashSet<int>> PoolToSpareSlotIndices_;
    THashMap<TString, int> PoolToMinUnusedSlotIndex_;

    TOperationElementMap OperationIdToElement_;

    THashMap<TOperationId, TInstant> OperationIdToActivationTime_;

    std::vector<TOperationId> ActivatableOperationQueue_;

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
        TRawOperationElementMap OperationIdToElement;
        TRawPoolMap PoolNameToElement;
        TFairShareStrategyTreeConfigPtr Config;
        THashSet<TOperationId> DisabledOperations;

        TOperationElement* FindOperationElement(TOperationId operationId) const;
        TPool* FindPool(const TString& poolName) const;
    };

    typedef TIntrusivePtr<TRootElementSnapshot> TRootElementSnapshotPtr;

    class TFairShareTreeSnapshot
        : public IFairShareTreeSnapshot
    {
    public:
        TFairShareTreeSnapshot(
            TFairShareTreePtr tree,
            TRootElementSnapshotPtr rootElementSnapshot,
            TSchedulingTagFilter nodesFilter,
            TJobResources totalResourceLimits,
            const NLogging::TLogger& logger);

        virtual TFuture<void> ScheduleJobs(const ISchedulingContextPtr& schedulingContext) override;

        virtual void PreemptJobsGracefully(const ISchedulingContextPtr& schedulingContext) override;

        virtual void ProcessUpdatedJob(TOperationId operationId, TJobId jobId, const TJobResources& delta) override;

        virtual void ProcessFinishedJob(TOperationId operationId, TJobId jobId) override;

        virtual void ApplyJobMetricsDelta(TOperationId operationId, const TJobMetrics& jobMetricsDelta) override;

        virtual void ProfileFairShare() const override;

        virtual bool HasOperation(TOperationId operationId) const override;

        virtual bool IsOperationDisabled(TOperationId operationId) const override;

        virtual const TSchedulingTagFilter& GetNodesFilter() const override;

        virtual TJobResources GetTotalResourceLimits() const override;

        virtual std::optional<TSchedulerElementStateSnapshot> GetMaybeStateSnapshotForPool(const TString& poolId) const override;

    private:
        const TIntrusivePtr<TFairShareTree> Tree_;
        const TRootElementSnapshotPtr RootElementSnapshot_;
        const TSchedulingTagFilter NodesFilter_;
        const TJobResources TotalResourceLimits_;
        const NLogging::TLogger Logger;
    };

    NConcurrency::TReaderWriterSpinLock GlobalDynamicAttributesLock_;
    TDynamicAttributesList GlobalDynamicAttributes_;
    THashMap<TString, int> ElementIndexes_;

    TRootElementSnapshotPtr RootElementSnapshot_;

    TFairShareSchedulingStage NonPreemptiveSchedulingStage_;
    TFairShareSchedulingStage PreemptiveSchedulingStage_;
    TFairShareSchedulingStage PackingFallbackSchedulingStage_;

    NProfiling::TAggregateGauge FairSharePreUpdateTimeCounter_;
    NProfiling::TAggregateGauge FairShareUpdateTimeCounter_;
    NProfiling::TAggregateGauge FairShareLogTimeCounter_;
    NProfiling::TAggregateGauge AnalyzePreemptableJobsTimeCounter_;

    TSpinLock CustomProfilingCountersLock_;
    THashMap<TString, std::unique_ptr<NProfiling::TAggregateGauge>> CustomProfilingCounters_;

    NProfiling::TCpuInstant LastSchedulingInformationLoggedTime_ = 0;

    bool IsBeingRemoved_ = false;

    TDynamicAttributes GetGlobalDynamicAttributes(const TSchedulerElement* element) const;

    std::pair<IFairShareTreeSnapshotPtr, TError> DoFairShareUpdateAt(TInstant now);

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

    void DoPreemptJobsGracefully(
        const ISchedulingContextPtr& schedulingContext,
        const TRootElementSnapshotPtr& rootElementSnapshot);

    void DoProfileFairShare(const TRootElementSnapshotPtr& rootElementSnapshot) const;

    void PreemptJob(
        const TJobPtr& job,
        const TOperationElementPtr& operationElement,
        const ISchedulingContextPtr& schedulingContext) const;

    TCompositeSchedulerElement* FindPoolViolatingMaxRunningOperationCount(TCompositeSchedulerElement* pool);
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

    TPoolPtr FindPool(const TString& id) const;
    TPoolPtr GetPool(const TString& id) const;
    TPool* FindRecentPoolSnapshot(const TString& id) const;

    TPoolPtr GetOrCreatePool(const TPoolName& poolName, TString userName);

    NProfiling::TTagId GetPoolProfilingTag(const TString& id);

    TOperationElementPtr FindOperationElement(TOperationId operationId) const;
    TOperationElementPtr GetOperationElement(TOperationId operationId) const;
    TOperationElement* FindRecentOperationElementSnapshot(TOperationId operationId) const;

    TCompositeSchedulerElement* GetRecentRootSnapshot() const;

    void BuildEssentialPoolsInformation(NYTree::TFluentMap fluent);
    void BuildElementYson(const TSchedulerElement* element, NYTree::TFluentMap fluent);
    void BuildEssentialElementYson(const TSchedulerElement* element, NYTree::TFluentMap fluent, bool shouldPrintResourceUsage);
    void BuildEssentialPoolElementYson(const TSchedulerElement* element, NYTree::TFluentMap fluent);
    void BuildEssentialOperationElementYson(const TSchedulerElement* element, NYTree::TFluentMap fluent);

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
