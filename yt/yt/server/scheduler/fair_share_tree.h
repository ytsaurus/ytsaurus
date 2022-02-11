#pragma once

#include "private.h"
#include "scheduler_tree_structs.h"
#include "scheduler_strategy.h"

#include <yt/yt/server/lib/scheduler/resource_metering.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategyOperationState
    : public TRefCounted
{
public:
    using TTreeIdToPoolNameMap = THashMap<TString, TPoolName>;

    DEFINE_BYVAL_RO_PROPERTY(IOperationStrategyHost*, Host);
    DEFINE_BYVAL_RO_PROPERTY(TFairShareStrategyOperationControllerPtr, Controller);
    DEFINE_BYREF_RW_PROPERTY(TTreeIdToPoolNameMap, TreeIdToPoolNameMap);
    DEFINE_BYVAL_RW_PROPERTY(bool, Enabled);

public:
    TFairShareStrategyOperationState(
        IOperationStrategyHost* host,
        const TFairShareStrategyOperationControllerConfigPtr& config,
        int NodeShardCount);

    void UpdateConfig(const TFairShareStrategyOperationControllerConfigPtr& config);

    TPoolName GetPoolNameByTreeId(const TString& treeId) const;
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyOperationState)

THashMap<TString, TPoolName> GetOperationPools(const TOperationRuntimeParametersPtr& runtimeParameters);

////////////////////////////////////////////////////////////////////////////////

struct TPoolsUpdateResult
{
    TError Error;
    bool Updated;
};

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerElementStateSnapshot
{
    TResourceVector DemandShare;
    TResourceVector PromisedFairShare;
};

////////////////////////////////////////////////////////////////////////////////

struct IFairShareTree
    : public virtual TRefCounted
{
    //! Methods below rely on presence of snapshot.
    virtual TFuture<void> ScheduleJobs(const ISchedulingContextPtr& schedulingContext) = 0;
    virtual void PreemptJobsGracefully(const ISchedulingContextPtr& schedulingContext) = 0;
    virtual void ProcessUpdatedJob(
        TOperationId operationId,
        TJobId jobId,
        const TJobResources& jobResources,
        const std::optional<TString>& jobDataCenter,
        const std::optional<TString>& jobInfinibandCluster,
        bool* shouldAbortJob) = 0;
    virtual bool ProcessFinishedJob(TOperationId operationId, TJobId jobId) = 0;

    virtual bool IsSnapshottedOperationRunningInTree(TOperationId operationId) const = 0;

    virtual TFairShareStrategyTreeConfigPtr GetSnapshottedConfig() const = 0;
    virtual TJobResources GetSnapshottedTotalResourceLimits() const = 0;
    virtual std::optional<TSchedulerElementStateSnapshot> GetMaybeStateSnapshotForPool(const TString& poolId) const = 0;
    virtual TCachedJobPreemptionStatuses GetCachedJobPreemptionStatuses() const = 0;

    virtual void ApplyJobMetricsDelta(THashMap<TOperationId, TJobMetrics> jobMetricsPerOperation) = 0;

    virtual void BuildResourceMetering(
        TMeteringMap* meteringMap,
        THashMap<TString, TString>* customMeteringTags) const = 0;

    virtual void ProfileFairShare() const = 0;
    virtual void LogFairShareAt(TInstant now) const = 0;
    virtual void EssentialLogFairShareAt(TInstant now) const = 0;

    //! Updates accumulated resources usage information. Update current resource usages in snapshot.
    virtual void UpdateResourceUsages() = 0;

    //! Updates fair share attributes of tree elements and saves it as tree snapshot.
    virtual TFuture<std::pair<IFairShareTreePtr, TError>> OnFairShareUpdateAt(TInstant now) = 0;
    virtual void FinishFairShareUpdate() = 0;

    //! Methods below manipute directly with tree structure and fields, it should be used in serialized manner.
    virtual TFairShareStrategyTreeConfigPtr GetConfig() const = 0;
    virtual bool UpdateConfig(const TFairShareStrategyTreeConfigPtr& config) = 0;
    virtual void UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config) = 0;

    virtual const TSchedulingTagFilter& GetNodesFilter() const = 0;
    
    virtual bool HasOperation(TOperationId operationId) const = 0;
    virtual bool HasRunningOperation(TOperationId operationId) const = 0;
    virtual int GetOperationCount() const = 0;

    virtual void RegisterOperation(
        const TFairShareStrategyOperationStatePtr& state,
        const TStrategyOperationSpecPtr& spec,
        const TOperationFairShareTreeRuntimeParametersPtr& runtimeParameters) = 0;
    virtual void UnregisterOperation(const TFairShareStrategyOperationStatePtr& state) = 0;

    virtual void EnableOperation(const TFairShareStrategyOperationStatePtr& state) = 0;
    virtual void DisableOperation(const TFairShareStrategyOperationStatePtr& state) = 0;

    virtual void ChangeOperationPool(
        TOperationId operationId,
        const TFairShareStrategyOperationStatePtr& state,
        const TPoolName& newPool) = 0;

    virtual void UpdateOperationRuntimeParameters(
        TOperationId operationId,
        const TOperationFairShareTreeRuntimeParametersPtr& runtimeParameters) = 0;

    virtual void RegisterJobsFromRevivedOperation(TOperationId operationId, const std::vector<TJobPtr>& jobs) = 0;

    virtual TError CheckOperationIsHung(
        TOperationId operationId,
        TDuration safeTimeout,
        int minScheduleJobCallAttempts,
        const THashSet<EDeactivationReason>& deactivationReasons,
        TDuration limitingAncestorSafeTimeout) = 0;

    virtual void ProcessActivatableOperations() = 0;
    virtual void TryRunAllPendingOperations() = 0;

    virtual TPoolName CreatePoolName(const std::optional<TString>& poolFromSpec, const TString& user) const = 0;

    virtual TPoolsUpdateResult UpdatePools(const NYTree::INodePtr& poolsNode, bool forceUpdate) = 0;
    virtual TError ValidateUserToDefaultPoolMap(const THashMap<TString, TString>& userToDefaultPoolMap) = 0;

    virtual void ValidatePoolLimits(const IOperationStrategyHost* operation, const TPoolName& poolName) const = 0;
    virtual void ValidatePoolLimitsOnPoolChange(const IOperationStrategyHost* operation, const TPoolName& newPoolName) const = 0;
    virtual TFuture<void> ValidateOperationPoolsCanBeUsed(const IOperationStrategyHost* operation, const TPoolName& poolName) const = 0;

    virtual void ActualizeEphemeralPoolParents(const THashMap<TString, TString>& userToDefaultPoolMap) = 0;

    virtual TPersistentTreeStatePtr BuildPersistentTreeState() const = 0;
    virtual void InitPersistentTreeState(const TPersistentTreeStatePtr& persistentTreeState) = 0;

    virtual ESchedulingSegment InitOperationSchedulingSegment(TOperationId operationId) = 0;
    virtual TTreeSchedulingSegmentsState GetSchedulingSegmentsState() const = 0;
    virtual TOperationIdWithSchedulingSegmentModuleList GetOperationSchedulingSegmentModuleUpdates() const = 0;

    virtual void BuildOperationAttributes(TOperationId operationId, NYTree::TFluentMap fluent) const = 0;
    virtual void BuildOperationProgress(TOperationId operationId, NYTree::TFluentMap fluent) const = 0;
    virtual void BuildBriefOperationProgress(TOperationId operationId, NYTree::TFluentMap fluent) const = 0;

    virtual void BuildStaticPoolsInformation(NYTree::TFluentAny fluent) const = 0;
    virtual void BuildUserToEphemeralPoolsInDefaultPool(NYTree::TFluentAny fluent) const = 0;

    virtual void BuildFairShareInfo(NYTree::TFluentMap fluent) const = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;

    //! Raised when operation considered running in tree.
    DECLARE_INTERFACE_SIGNAL(void(TOperationId), OperationRunning);
};

DEFINE_REFCOUNTED_TYPE(IFairShareTree)

////////////////////////////////////////////////////////////////////////////////

IFairShareTreePtr CreateFairShareTree(
    TFairShareStrategyTreeConfigPtr config,
    TFairShareStrategyOperationControllerConfigPtr controllerConfig,
    ISchedulerStrategyHost* strategyHost,
    std::vector<IInvokerPtr> feasibleInvokers,
    TString treeId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
