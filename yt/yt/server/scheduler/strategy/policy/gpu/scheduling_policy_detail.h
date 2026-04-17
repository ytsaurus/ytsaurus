#pragma once

#include "assignment_plan_update_context_detail.h"
#include "persistent_state.h"

#include <yt/yt/server/scheduler/strategy/policy/scheduling_policy.h>

#include <yt/yt/server/scheduler/strategy/pool_tree_element.h>

#include <yt/yt/server/scheduler/strategy/scheduling_heartbeat_context.h>

#include <yt/yt/server/scheduler/common/allocation.h>

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/virtual.h>


namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NYTree;
using namespace NYson;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

struct TAllocationInfo
{
    TAllocationPtr Allocation;
    TPoolTreeOperationElement* OperationElement = nullptr;
};

using TAllocationInfoMap = THashMap<TAllocationId, TAllocationInfo>;

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
    NProfiling::TEventTimer RegularPlanningTime;
    NProfiling::TEventTimer ExtraPlanningTime;

    NProfiling::TGauge EnabledOperations;
    NProfiling::TGauge FullHostModuleBoundOperations;

    NProfiling::TGauge AssignedGpu;

    THashMap<std::string, TModuleProfilingCounters> ModuleCounters;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulingPolicy
    : public ISchedulingPolicy
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

    TProcessAllocationUpdateResult ProcessAllocationUpdate(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        TPoolTreeOperationElement* element,
        const TAllocationUpdate& allocationUpdate) override;

    void BuildSchedulingAttributesStringForNode(
        TNodeId nodeId,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const override;

    void BuildSchedulingAttributesForNode(TNodeId nodeId, TFluentMap fluent) const override;

    // TODO(yaishenka): implement these methods in YT-27633.
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

private:
    const TWeakPtr<ISchedulingPolicyHost> Host_;
    IStrategyHost* const StrategyHost_;

    const NLogging::TLogger Logger;

    TGpuSchedulingPolicyConfigPtr Config_;

    TPeriodicExecutorPtr PlanUpdateExecutor_;
    TAssignmentHandler AssignmentHandler_;

    TNodeMap Nodes_;
    TOperationMap EnabledOperations_;
    TOperationMap DisabledOperations_;

    TInstant InitializationFromPersistentStateDeadline_;
    TPersistentStatePtr InitialPersistentState_ = New<TPersistentState>();
    TPersistentStatePtr PersistentState_;

    NProfiling::TProfiler Profiler_;
    TGpuSchedulingProfilingCounters ProfilingCounters_;

    TGpuPlanUpdateStatisticsPtr Statistics_;

    // TODO(YT-27647): Refactor after switching to node shard paradigm.
    NConcurrency::TAsyncReaderWriterLock AssignmentPlanUpdateLock_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void UpdateNodeDescriptor(const TNodePtr& node, TExecNodeDescriptorPtr descriptor);

    void UpdateAssignmentPlan();

    void PreemptAllNodeAssignments(
        const TNodePtr& node,
        EAllocationPreemptionReason preemptionReason,
        const std::string& preemptionDescription);

    void PreemptAllOperationAssignments(
        const TOperationPtr& operation,
        EAllocationPreemptionReason preemptionReason,
        const std::string& preemptionDescription);

    void PreemptAssignment(
        const TAssignmentPtr& assignment,
        EAllocationPreemptionReason preemptionReason,
        const std::string& preemptionDescription);

    void RemoveAssignment(const TAssignmentPtr& assignment, bool strict = true);

    void ReviveNodeState(const TNodePtr& node);

    void ReviveOperationState(TOperationPtr operation);

    //! Returns false if Now > InitializationFromPersistentStateDeadline_ and drops persistentState
    //! Returns false if InitialPersistentState_ is empty
    //! Returns true otherwise
    bool CheckInitializationTimeout();

    std::optional<TPersistentNodeState> FindInitialNodePersistentState(TNodeId nodeId);

    std::optional<TPersistentOperationState> FindInitialOperationPersistentState(TOperationId operationId);

    void UpdatePersistentState();

    void LogSnapshotEvent(const TGpuPlanUpdateStatisticsPtr& statistics) const;

    void ProfileAssignmentPlanUpdating(const TGpuPlanUpdateStatisticsPtr& statistics);

    TLogger GetNodeLogger(const TExecNodeDescriptorPtr& nodeDescriptor);

    void DoProcessSchedulingHeartbeat(
        const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
        const TPoolTreeSnapshotPtr& treeSnapshot,
        bool skipScheduleAllocations);

    void PreemptAllocations(
        const TNodePtr& node,
        const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
        const TPoolTreeSnapshotPtr& treeSnapshot);

    void ScheduleAllocations(
        const TNodePtr& node,
        const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
        const TPoolTreeSnapshotPtr& treeSnapshot);

    void PreemptAllocation(
        const TAllocationPtr& allocation,
        TPoolTreeOperationElement* element,
        const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
        EAllocationPreemptionReason preemptionReason,
        TJobResources preemptedUsage) const;

    TControllerScheduleAllocationResultPtr DoScheduleAllocation(
        const TNodePtr& node,
        const TOperationPtr& operation,
        TPoolTreeOperationElement* element,
        const TAssignmentPtr& assignment,
        const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TJobResources& availableResources);

    TProcessAllocationUpdateResult DoProcessAllocationUpdate(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        TPoolTreeOperationElementPtr element,
        const TAllocationUpdate& allocationUpdate);

    void DoBuildSchedulingAttributesForNode(TNodeId nodeId, TFluentMap fluent) const;
    void DoBuildSchedulingAttributesStringForNode(TNodeId nodeId, TStringBuilderBase* builder) const;
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

    TProcessAllocationUpdateResult ProcessAllocationUpdate(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        TPoolTreeOperationElement* element,
        const TAllocationUpdate& allocationUpdate) override;

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
