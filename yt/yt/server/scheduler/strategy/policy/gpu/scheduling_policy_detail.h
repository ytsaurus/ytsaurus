#pragma once

#include "scheduling_policy.h"

#include "assignment_plan_context_detail.h"
#include "persistent_state.h"

#include <yt/yt/server/scheduler/strategy/pool_tree_element.h>

#include <yt/yt/server/scheduler/common/allocation.h>

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/virtual.h>


namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NYTree;
using namespace NYson;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

struct TModuleProfilingCounters
{
    explicit TModuleProfilingCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TGauge TotalModuleNodes;
    NProfiling::TGauge ModuleUnreservedNodes;
    NProfiling::TGauge ModuleFullHostModuleBoundOperations;
};

////////////////////////////////////////////////////////////////////////////////

struct TGpuSchedulingProfilingCounters
{
    explicit TGpuSchedulingProfilingCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter PlannedAssignments;
    NProfiling::TCounter PreemptedAssignments;
    NProfiling::TGauge Assignments;

    NProfiling::TEventTimer TotalPlanningTime;
    NProfiling::TEventTimer OperationResourcesUpdateTime;
    NProfiling::TEventTimer FullHostPlanningTime;
    NProfiling::TEventTimer ReguralPlanningTime;
    NProfiling::TEventTimer ExtraPlanningTime;

    NProfiling::TGauge EnabledOperations;
    NProfiling::TGauge FullHostModuleBoundOperations;

    NProfiling::TGauge AssignedGpu;

    THashMap<std::string, TModuleProfilingCounters> ModuleCounters;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulingPolicy
    : public ISchedulingPolicy
    , public TAssignmentPlanContextBase
{
public:
    TSchedulingPolicy(
        TWeakPtr<ISchedulingPolicyHost> host,
        IStrategyHost* strategyHost,
        const std::string& treeId,
        TGpuSchedulingPolicyConfigPtr config,
        NProfiling::TProfiler profiler);

    void Initialize() override;

    void RegisterNode(TNodeId nodeId, const std::string& nodeAddress) override;
    void UnregisterNode(TNodeId nodeId) override;
    void UpdateNodeDescriptor(TNodeId nodeId, TExecNodeDescriptorPtr descriptor) override;

    void ProcessSchedulingHeartbeat(
        const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
        const TPoolTreeSnapshotPtr& treeSnapshot,
        bool skipScheduleAllocations) override;

    void RegisterOperation(const TPoolTreeOperationElement* element) override;
    void UnregisterOperation(const TPoolTreeOperationElement* element) override;
    TError OnOperationMaterialized(const TPoolTreeOperationElement* element) override;
    TError CheckOperationSchedulingInSeveralTreesAllowed(const TPoolTreeOperationElement* element) const override;
    void EnableOperation(const TPoolTreeOperationElement* element) override;
    void DisableOperation(TPoolTreeOperationElement* element, bool markAsNonAlive) override;

    void RegisterAllocationsFromRevivedOperation(
        TPoolTreeOperationElement* element,
        std::vector<TAllocationPtr> allocations) const override;

    bool ProcessAllocationUpdate(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        TPoolTreeOperationElement* element,
        TAllocationId allocationId,
        const TJobResources& allocationResources,
        bool resetPreemptibleProgress,
        const std::optional<std::string>& allocationDataCenter,
        const std::optional<std::string>& allocationInfinibandCluster,
        std::optional<EAbortReason>* maybeAbortReason) const override;

    bool ProcessFinishedAllocation(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        TPoolTreeOperationElement* element,
        TAllocationId allocationId) const override;

    void BuildSchedulingAttributesStringForNode(
        TNodeId nodeId,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const override;

    void BuildSchedulingAttributesForNode(TNodeId nodeId, TFluentMap fluent) const override;

    void BuildSchedulingAttributesStringForOngoingAllocations(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const std::vector<TAllocationPtr>& allocations,
        TInstant now,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const override;

    void BuildElementLoggingStringAttributes(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TPoolTreeElement* element,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const override;

    void PopulateOrchidService(const TCompositeMapServicePtr& orchidService) const override;

    void ProfileOperation(
        const TPoolTreeOperationElement* element,
        const TPoolTreeSnapshotPtr& treeSnapshot,
        NProfiling::ISensorWriter* writer) const override;

    TPostUpdateContextPtr CreatePostUpdateContext(TPoolTreeRootElement* rootElement) override;
    void PostUpdate(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TPostUpdateContextPtr* postUpdateContext) override;

    TPoolTreeSnapshotStatePtr CreateSnapshotState(TPostUpdateContextPtr* postUpdateContext) override;

    void OnResourceUsageSnapshotUpdate(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot) const override;

    void UpdateConfig(TStrategyTreeConfigPtr treeConfig) override;

    void InitPersistentState(INodePtr persistentState) override;
    INodePtr BuildPersistentState() const override;

    const TOperationMap& Operations() const override;
    const TNodeMap& Nodes() const override;
    TGpuPlanUpdateStatisticsPtr Statistics() const override;

private:
    const TWeakPtr<ISchedulingPolicyHost> Host_;
    IStrategyHost* const StrategyHost_;

    const NLogging::TLogger Logger;

    TGpuSchedulingPolicyConfigPtr Config_;

    TPeriodicExecutorPtr PlanUpdateExecutor_;

    TNodeMap Nodes_;
    TOperationMap EnabledOperations_;
    TOperationMap DisabledOperations_;

    TInstant InitializationFromPersistentStateDeadline_;
    TPersistentStatePtr InitialPersistentState_ = New<TPersistentState>();
    TPersistentStatePtr PersistentState_;

    THashMap<TOperationId, THashSet<TPersistentAssignmentStatePtr>> InitialOperationAssignments_;

    NProfiling::TProfiler Profiler_;
    TGpuSchedulingProfilingCounters ProfilingCounters_;

    TGpuPlanUpdateStatisticsPtr Statistics_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void UpdateAssignmentPlan();

    // TODO(eshcherbin): Optimize not to recalculate preemptible assignments and ready to assign resources from scratch.
    void UpdateOperationResources(
        const TOperationPtr& operation,
        const TPoolTreeSnapshotPtr& treeSnapshot);

    TAllocationGroupResourcesMap GetGroupedNeededResources(
        const TOperationPtr& operation,
        const TPoolTreeOperationElement* operationElement) const;

    void ResetOperationResources(const TOperationPtr& operation);

    void PreemptAllNodeAssignments(
        const TNodePtr& node,
        EAllocationPreemptionReason preemptionReason,
        const std::string& preemptionDescription);

    void PreemptAllOperationAssignments(
        const TOperationPtr& operation,
        EAllocationPreemptionReason preemptionReason,
        const std::string& preemptionDescription);

    void ReviveNodeState(TNodeId nodeId, const TNodePtr& node);

    void ReviveOperationState(TOperationPtr operation);

    //! Returns false if Now > InitializationFromPersistentStateDeadline_ and drops persistentState
    //! Returns false if InitialPersistentState_ is empty
    //! Returns true otherwise
    bool CheckInitializationTimeout();

    std::optional<TPersistentNodeState> FindInitialNodePersistentState(TNodeId nodeId);

    std::optional<TPersistentOperationState> FindInitialOperationPersistentState(TOperationId operationId);

    void UpdatePersistentState();

    void LogSnapshotEvent() const;

    void ProfileAssignmentPlanUpdating();
};

DEFINE_REFCOUNTED_TYPE(TSchedulingPolicy)

////////////////////////////////////////////////////////////////////////////////

class TNoopSchedulingPolicy
    : public ISchedulingPolicy
{
public:
    explicit TNoopSchedulingPolicy(const std::string& treeId);

    void Initialize() override;

    void RegisterNode(TNodeId nodeId, const std::string& nodeAddress) override;
    void UnregisterNode(TNodeId nodeId) override;
    void UpdateNodeDescriptor(TNodeId nodeId, TExecNodeDescriptorPtr descriptor) override;

    void RegisterOperation(const TPoolTreeOperationElement* element) override;
    void UnregisterOperation(const TPoolTreeOperationElement* element) override;
    TError OnOperationMaterialized(const TPoolTreeOperationElement* element) override;
    void EnableOperation(const TPoolTreeOperationElement* element) override;
    void DisableOperation(TPoolTreeOperationElement* element, bool markAsNonAlive) override;

    void PopulateOrchidService(const TCompositeMapServicePtr& orchidService) const override;

    void UpdateConfig(TStrategyTreeConfigPtr config) override;

    void InitPersistentState(INodePtr persistentState) override;
    INodePtr BuildPersistentState() const override;

    TPostUpdateContextPtr CreatePostUpdateContext(TPoolTreeRootElement* rootElement) override;
    void PostUpdate(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TPostUpdateContextPtr* postUpdateContext) override;

    TPoolTreeSnapshotStatePtr CreateSnapshotState(TPostUpdateContextPtr* postUpdateContext) override;

    void OnResourceUsageSnapshotUpdate(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot) const override;

    void ProcessSchedulingHeartbeat(
        const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
        const TPoolTreeSnapshotPtr& treeSnapshot,
        bool skipScheduleAllocations) override;

    TError CheckOperationSchedulingInSeveralTreesAllowed(const TPoolTreeOperationElement* element) const override;

    void RegisterAllocationsFromRevivedOperation(
        TPoolTreeOperationElement* element,
        std::vector<TAllocationPtr> allocations) const override;

    bool ProcessAllocationUpdate(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        TPoolTreeOperationElement* element,
        TAllocationId allocationId,
        const TJobResources& allocationResources,
        bool resetPreemptibleProgress,
        const std::optional<std::string>& allocationDataCenter,
        const std::optional<std::string>& allocationInfinibandCluster,
        std::optional<EAbortReason>* maybeAbortReason) const override;

    bool ProcessFinishedAllocation(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        TPoolTreeOperationElement* element,
        TAllocationId allocationId) const override;

    void BuildSchedulingAttributesStringForNode(
        TNodeId nodeId,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const override;

    void BuildSchedulingAttributesForNode(TNodeId nodeId, TFluentMap fluent) const override;

    void BuildSchedulingAttributesStringForOngoingAllocations(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const std::vector<TAllocationPtr>& allocations,
        TInstant now,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const override;

    void BuildElementLoggingStringAttributes(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TPoolTreeElement* element,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const override;

    void ProfileOperation(
        const TPoolTreeOperationElement* element,
        const TPoolTreeSnapshotPtr& treeSnapshot,
        NProfiling::ISensorWriter* writer) const override;

private:
    const TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TNoopSchedulingPolicy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
