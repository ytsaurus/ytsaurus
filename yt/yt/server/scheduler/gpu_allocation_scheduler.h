#pragma once

#include "public.h"
#include "gpu_allocation_scheduler_structs.h"
#include "scheduler_strategy.h"
#include "fair_share_tree_allocation_scheduler_structs.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TGpuAllocationSchedulerHost
{
public:
    virtual void ResetOperationModule(const TGpuSchedulerOperationStatePtr& operation) = 0;

    virtual void RemoveOperationFromNodes(const TGpuSchedulerOperationStatePtr& operation) = 0;
    virtual void RemoveAllAllocationsFromNode(const TGpuSchedulerNodeStatePtr& node) = 0;
    virtual void RemoveAllocationFromNode(const TGpuAllocationStatePtr& allocation, const TGpuSchedulerNodeStatePtr& node) = 0;

    virtual const TSchedulingModule& GetNodeModule(const TGpuSchedulerNodeState& node) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TGpuSchedulingContext
{
public:
    TGpuSchedulingContext(
        TGpuAllocationSchedulerHost* schedulerHost,
        TGpuSchedulerOperationStateMap* operationStates,
        TGpuSchedulerNodeStateMap* nodeStates,
        TInstant now,
        NLogging::TLogger logger,
        TGpuAllocationSchedulerConfigPtr config);

    void ScheduleAllocations();

private:
    const TInstant Now_;

    const NLogging::TLogger Logger;

    TGpuAllocationSchedulerConfigPtr Config_;

    TGpuAllocationSchedulerHost* SchedulerHost_;

    TGpuSchedulerOperationStateMap* OperationStates_;
    TGpuSchedulerNodeStateMap* NodeStates_;

    // Large operations that are ready for module assignment.
    std::vector<TGpuSchedulerOperationStatePtr> LargeOperationsToAssign_;

    // Operations that are ready for scheduling.
    THashMap<TString, THashSet<TGpuSchedulerOperationStatePtr>> LargeOperationsToSchedulePerModule_;
    THashSet<TGpuSchedulerOperationStatePtr> SmallOperationsToSchedule_;

    // NB(omgronny): SortedNodeStates initializes after the module assignment.
    using TNodeComparator = std::function<bool(const TGpuSchedulerNodeStatePtr&, const TGpuSchedulerNodeStatePtr&)>;
    std::optional<std::set<TGpuSchedulerNodeStatePtr, TNodeComparator>> SortedNodeStates_;

    THashMap<TSchedulingModule, double> TotalCapacityPerModule_;
    THashMap<TSchedulingModule, double> RemainingCapacityPerModule_;

    void PrepareGpuSchedulingContext();
    void AssignOperationsToModules();
    void PrepareNodeStatesInGpuSchedulingContext();
    void ScheduleLargeAllocationsToNodes();
    void ScheduleSmallAllocationsToNodes();

    void DoScheduleAllocationsToNodes(const THashSet<TGpuSchedulerOperationStatePtr>& operations);

    bool IsLargeOperation(const TGpuSchedulerOperationStatePtr& operation) const;
    bool IsOperationReady(const TGpuSchedulerOperationStatePtr& operation) const;

    void SetOperationEligibleForPriorityModuleAssignment(const TGpuSchedulerOperationStatePtr& operation);

    void ResetOperationModule(const TGpuSchedulerOperationStatePtr& operation);
    void PreemptNonPriorityOperationsFromModuleForOperation(
        TOperationId priorityOperationId,
        const std::vector<TGpuSchedulerOperationStatePtr>& operations);

    struct TNodeWithSchedulingInfo
    {
        TGpuSchedulerNodeStatePtr Node;
        TJobResourcesWithQuota ScheduledResources;
        std::vector<TGpuAllocationStatePtr> Allocations;
        THashSet<TGpuAllocationStatePtr> AllocationsToPreempt;
    };

    struct TBestNodeForAllocation
    {
        TGpuSchedulerNodeStatePtr Node;
        THashSet<TGpuAllocationStatePtr> AllocationsToPreempt;
    };

    TBestNodeForAllocation FindBestSuitableNodeForAllocation(
        const TGpuSchedulerOperationStatePtr& operation,
        const TGpuAllocationStatePtr& allocation,
        const THashMap<NNodeTrackerClient::TNodeId, TNodeWithSchedulingInfo>& scheduledResourcesPerNode);
    bool IsNodeWithSmallAllocations(const TGpuSchedulerNodeStatePtr& node) const;

    void UpdateOperationAllocationsToSchedule(const TGpuSchedulerOperationStatePtr& operation);

    struct TOperationsToPreempt
    {
        double TotalPenalty = 0.0;
        std::vector<TGpuSchedulerOperationStatePtr> Operations;
        TSchedulingModule Module;
    };
    std::optional<TOperationsToPreempt> FindBestOperationsToPreemptInModuleGreedy(
        const TSchedulingModule& module,
        double neededDemand);
    std::optional<TOperationsToPreempt> FindBestOperationsToPreempt(const TGpuSchedulerOperationStatePtr& operation);

    void SetNodeUsage(const TGpuSchedulerNodeStatePtr& node, const TJobResources& usage);
    bool CanSatisfyDiskQuotaRequests(
        const TDiskResources& diskResources,
        const THashMap<TGpuAllocationStatePtr, TDiskQuota>& diskQuotaRequests);

    bool CompareGpuSchedulerOperationStatesForPreSchedulingSort(
        const TGpuSchedulerOperationStatePtr& lhs,
        const TGpuSchedulerOperationStatePtr& rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

class TGpuAllocationScheduler
    : public TRefCounted
    , public TGpuAllocationSchedulerHost
{
public:
    TGpuAllocationScheduler(
        IInvokerPtr invoker,
        TGpuAllocationSchedulerConfigPtr config,
        NLogging::TLogger logger);

    // TODO(omgronny): Node descriptor may change.
    void RegisterNode(NNodeTrackerClient::TNodeId nodeId, const TFairShareTreeAllocationSchedulerNodeState& node);
    void UnregisterNode(NNodeTrackerClient::TNodeId nodeId);

    void RegisterOperation(
        TOperationId operationId,
        bool isGang,
        std::optional<THashSet<TString>> specifiedSchedulingModules = {});
    void UnregisterOperation(TOperationId operationId);

    void UpdateOperationRuntimeAttributes(TOperationId operationId, TOperationRuntimeAttributes attributes);
    void UpdateOperationMinNeededResources(TOperationId operationId, TJobResources minNeededResources);

    void ScheduleAllocations();

    void UpdateConfig(TGpuAllocationSchedulerConfigPtr config);

    THashSet<TGpuAllocationStatePtr> GetScheduledAllocationsForNode(NNodeTrackerClient::TNodeId nodeId) const;

    void OnAllocationFinished(TOperationId operationId, const TGpuAllocationStatePtr& allocation, NNodeTrackerClient::TNodeId nodeId);

    // NB(omgronny): Only for testing.
    THashMap<NNodeTrackerClient::TNodeId, THashSet<TGpuAllocationStatePtr>> GetScheduledAllocations() const;
    TGpuSchedulerNodeStatePtr GetNodeState(NNodeTrackerClient::TNodeId nodeId) const;
    TGpuSchedulerOperationStatePtr GetOperationState(TOperationId operationId);

private:
    const IInvokerPtr Invoker_;

    const NLogging::TLogger Logger;

    TGpuAllocationSchedulerConfigPtr Config_;

    // Owning map for all operations.
    TGpuSchedulerOperationStateMap OperationStates_;

    // Owning map for all nodes.
    TGpuSchedulerNodeStateMap NodeStates_;

    void ResetOperationModuleAssignments(TInstant now);
    void ResetOperationModule(const TGpuSchedulerOperationStatePtr& operation) override;

    void RemoveOperationFromNodes(const TGpuSchedulerOperationStatePtr& operation) override;
    void RemoveAllAllocationsFromNode(const TGpuSchedulerNodeStatePtr& node) override;
    void RemoveAllocationFromNode(const TGpuAllocationStatePtr& allocation, const TGpuSchedulerNodeStatePtr& node) override;

    const TSchedulingModule& GetNodeModule(
        const std::optional<std::string>& nodeDataCenter,
        const std::optional<std::string>& nodeInfinibandCluster,
        ESchedulingModuleType moduleType) const;
    const TSchedulingModule& GetNodeModule(
        const TExecNodeDescriptorPtr& nodeDescriptor,
        ESchedulingModuleType moduleType) const;
    const TSchedulingModule& GetNodeModule(const TGpuSchedulerNodeState& node) const override;

    friend class TGpuSchedulingContext;
};

DEFINE_REFCOUNTED_TYPE(TGpuAllocationScheduler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
