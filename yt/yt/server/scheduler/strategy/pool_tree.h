#pragma once

#include "private.h"
#include "strategy.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/resource_metering.h>

#include <yt/yt/ytlib/scheduler/config.h>

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, TPoolName> GetOperationPools(const TOperationRuntimeParametersPtr& runtimeParameters);

////////////////////////////////////////////////////////////////////////////////

struct TPoolsUpdateResult
{
    TError Error;
    bool Updated;
};

////////////////////////////////////////////////////////////////////////////////

class TAccumulatedResourceDistribution
{
public:
    TAccumulatedResourceDistribution() = default;

    DEFINE_BYREF_RO_PROPERTY(TResourceVolume, FairResources);
    DEFINE_BYREF_RO_PROPERTY(TResourceVolume, Usage);
    DEFINE_BYREF_RO_PROPERTY(TResourceVolume, UsageDeficit);

    void AppendPeriod(const TJobResources& fairResources, const TJobResources& usage, TDuration period);
    TAccumulatedResourceDistribution& operator+=(const TAccumulatedResourceDistribution& other);
};

void Serialize(const TAccumulatedResourceDistribution& volume, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TPoolTreeElementStateSnapshot
{
    TResourceVector DemandShare;
    TResourceVector EstimatedGuaranteeShare;
};

////////////////////////////////////////////////////////////////////////////////

struct IPoolTreeHost
{
    virtual ~IPoolTreeHost() = default;

    virtual bool IsConnected() const = 0;

    virtual void SetSchedulerTreeAlert(const std::string& treeId, ESchedulerAlertType alertType, const TError& alert) = 0;

    virtual const re2::RE2& GetEphemeralPoolNameRegex() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IPoolTree
    : public virtual TRefCounted
{
    //! Methods below rely on presence of snapshot.
    virtual TFuture<void> ProcessSchedulingHeartbeat(const NPolicy::ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext, bool skipScheduleAllocations) = 0;
    virtual void ProcessAllocationUpdates(
        const std::vector<TAllocationUpdate>& allocationUpdates,
        THashSet<TAllocationId>* allocationsToPostpone,
        THashMap<TAllocationId, EAbortReason>* allocationsToAbort) = 0;

    virtual int GetSchedulingHeartbeatComplexity() const = 0;

    virtual bool IsSnapshottedOperationRunningInTree(TOperationId operationId) const = 0;

    virtual TStrategyTreeConfigPtr GetSnapshottedConfig() const = 0;
    virtual TJobResources GetSnapshottedTotalResourceLimits() const = 0;
    virtual std::optional<TPoolTreeElementStateSnapshot> GetMaybeStateSnapshotForPool(const TString& poolId) const = 0;
    virtual void BuildSchedulingAttributesStringForNode(NNodeTrackerClient::TNodeId nodeId, TDelimitedStringBuilderWrapper& delimitedBuilder) const = 0;
    virtual void BuildSchedulingAttributesForNode(NNodeTrackerClient::TNodeId nodeId, NYTree::TFluentMap fluent) const = 0;
    virtual void BuildSchedulingAttributesStringForOngoingAllocations(
        const std::vector<TAllocationPtr>& allocations,
        TInstant now,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const = 0;

    virtual void ApplyJobMetricsDelta(THashMap<TOperationId, TJobMetrics> jobMetricsPerOperation) = 0;

    virtual void BuildResourceMetering(
        TMeteringMap* meteringMap,
        THashMap<TString, TString>* customMeteringTags) const = 0;

    virtual void ProfileFairShare() const = 0;
    virtual void LogFairShareAt(TInstant now) const = 0;
    virtual void LogAccumulatedUsage() const = 0;
    virtual void EssentialLogFairShareAt(TInstant now) const = 0;

    //! Updates accumulated resources usage information. Update current resource usages in snapshot.
    virtual void UpdateResourceUsages() = 0;

    // Extracts accumulated usage for operation.
    virtual TAccumulatedResourceDistribution ExtractAccumulatedResourceDistributionForLogging(TOperationId operationId) = 0;

    //! Updates fair share attributes of tree elements and saves it as tree snapshot.
    virtual TFuture<std::pair<IPoolTreePtr, TError>> OnFairShareUpdateAt(TInstant now) = 0;
    virtual void FinishFairShareUpdate() = 0;

    //! Methods below manipulate directly with tree structure and fields, it should be used in serialized manner.
    virtual TStrategyTreeConfigPtr GetConfig() const = 0;
    virtual void UpdateControllerConfig(const TStrategyOperationControllerConfigPtr& config) = 0;

    virtual bool UpdateConfig(const TStrategyTreeConfigPtr& config) = 0;

    virtual const TSchedulingTagFilter& GetNodesFilter() const = 0;

    virtual bool HasOperation(TOperationId operationId) const = 0;
    virtual bool HasRunningOperation(TOperationId operationId) const = 0;
    virtual int GetOperationCount() const = 0;

    struct TRegistrationResult
    {
        bool AllowIdleCpuPolicy = false;
    };

    virtual TRegistrationResult RegisterOperation(
        const TStrategyOperationStatePtr& state,
        const TStrategyOperationSpecPtr& spec,
        const TOperationPoolTreeRuntimeParametersPtr& runtimeParameters,
        const TOperationOptionsPtr& operationOptions) = 0;
    virtual void UnregisterOperation(const TStrategyOperationStatePtr& state) = 0;

    virtual void EnableOperation(const TStrategyOperationStatePtr& state) = 0;
    virtual void DisableOperation(const TStrategyOperationStatePtr& state) = 0;

    virtual void ChangeOperationPool(
        TOperationId operationId,
        const TPoolName& newPool,
        bool ensureRunning) = 0;

    virtual void UpdateOperationRuntimeParameters(
        TOperationId operationId,
        TSchedulingTagFilter schedulingTagFilter,
        const TOperationPoolTreeRuntimeParametersPtr& runtimeParameters) = 0;

    virtual void RegisterAllocationsFromRevivedOperation(TOperationId operationId, std::vector<TAllocationPtr> allocations) = 0;

    virtual void RegisterNode(NNodeTrackerClient::TNodeId nodeId, const std::string& nodeAddress) = 0;
    virtual void UnregisterNode(NNodeTrackerClient::TNodeId nodeId) = 0;

    virtual const std::string& GetId() const = 0;

    virtual TError CheckOperationIsStuck(
        TOperationId operationId,
        const TOperationStuckCheckOptionsPtr& options) = 0;

    virtual void ProcessActivatableOperations() = 0;
    virtual void TryRunAllPendingOperations() = 0;

    virtual TPoolName CreatePoolName(const std::optional<TString>& poolFromSpec, const std::string& user) const = 0;
    virtual const TOffloadingSettings& GetOffloadingSettingsFor(const TString& poolName, const std::string& user) const = 0;

    virtual TPoolsUpdateResult UpdatePools(const NYTree::INodePtr& poolsNode, bool forceUpdate) = 0;
    virtual TError ValidateUserToDefaultPoolMap(const THashMap<std::string, TString>& userToDefaultPoolMap) = 0;

    virtual void ValidatePoolLimits(const IOperation* operation, const TPoolName& poolName) const = 0;
    virtual void ValidatePoolLimitsOnPoolChange(const IOperation* operation, const TPoolName& newPoolName) const = 0;
    virtual TFuture<void> ValidateOperationPoolsCanBeUsed(const IOperation* operation, const TPoolName& poolName) const = 0;
    virtual TFuture<void> ValidateOperationPoolPermissions(TOperationId operationId, const std::string& user, NYTree::EPermissionSet permissions) const = 0;
    virtual void EnsureOperationPoolExistence(const TString& poolName) const = 0;

    virtual void ActualizeEphemeralPoolParents(const THashMap<std::string, TString>& userToDefaultPoolMap) = 0;

    virtual TPersistentTreeStatePtr BuildPersistentState() const = 0;
    virtual void InitPersistentState(const TPersistentTreeStatePtr& persistentState) = 0;

    virtual TError OnOperationMaterialized(TOperationId operationId) = 0;
    virtual TError CheckOperationJobResourceLimitsRestrictions(TOperationId operationId, bool revivedFromSnapshot) = 0;
    virtual TError CheckOperationSchedulingInSeveralTreesAllowed(TOperationId operationId) const = 0;

    virtual void BuildOperationAttributes(TOperationId operationId, NYTree::TFluentMap fluent) const = 0;
    virtual void BuildOperationProgress(TOperationId operationId, NYTree::TFluentMap fluent) const = 0;
    virtual void BuildBriefOperationProgress(TOperationId operationId, NYTree::TFluentMap fluent) const = 0;

    virtual void BuildStaticPoolsInformation(NYTree::TFluentAny fluent) const = 0;
    virtual void BuildUserToEphemeralPoolsInDefaultPool(NYTree::TFluentAny fluent) const = 0;

    virtual void BuildFairShareInfo(NYTree::TFluentMap fluent) const = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;

    virtual const TJobResourcesByTagFilter& GetResourceLimitsByTagFilter() const = 0;

    //! Raised when operation considered running in tree.
    DECLARE_INTERFACE_SIGNAL(void(TOperationId), OperationRunning);
};

DEFINE_REFCOUNTED_TYPE(IPoolTree)

////////////////////////////////////////////////////////////////////////////////

IPoolTreePtr CreatePoolTree(
    TStrategyTreeConfigPtr config,
    TStrategyOperationControllerConfigPtr controllerConfig,
    IPoolTreeHost* host,
    IStrategyHost* strategyHost,
    std::vector<IInvokerPtr> feasibleInvokers,
    std::string treeId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
