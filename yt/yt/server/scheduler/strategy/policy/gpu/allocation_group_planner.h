#pragma once

#include "structs.h"

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

using TPreemptionPenalty = i64;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

NDetail::TPreemptionPenalty ComputeAssignmentPreemptionPenalty(
    const TAssignmentPtr& assignment,
    const TGpuSchedulingPolicyConfigPtr& config,
    TInstant now);

////////////////////////////////////////////////////////////////////////////////

class TAllocationGroupPlannerBase
{
public:
    DEFINE_BYREF_RO_PROPERTY(std::vector<TAssignmentPtr>, PlannedAssignments);

public:
    TAllocationGroupPlannerBase(
        TOperationPtr operation,
        std::string allocationGroupName,
        TAllocationGroupResources allocationGroupResources,
        IAssignmentPlanUpdateContext* context,
        NLogging::TLogger logger);

    virtual ~TAllocationGroupPlannerBase() = default;

    void Run();

    int GetPlannedAssignmentCount() const;

protected:
    const TOperationPtr Operation_;
    const std::string AllocationGroupName_;
    const TAllocationGroupResources AllocationGroupResources_;
    IAssignmentPlanUpdateContext* const Context_;
    const NLogging::TLogger Logger;

    bool CanAddAssignmentToNode(
        TNode* node,
        const TJobResources& discount = {}) const;
    virtual void AddAssignmentToNode(TNode* node);

private:
    //! Returns |nullptr| if there are no available nodes.
    virtual TNode* FindBestAvailableNode() = 0;

    virtual bool ShouldConsiderDiskUsage() const;

    //! Resources that must fit into the node's limits to add an assignment.
    TJobResources GetRequiredResources(TNode* node, const TJobResources& discount) const;
    bool CanSatisfyResourceRequest(TNode* node, const TJobResources& discount) const;
    //! Disk quota requests that must fit into the node's disk resources to add an assignment.
    std::vector<TDiskQuota> GetDiskRequests(TNode* node) const;
    //! Returns the disk requests that don't fit into the node, or |nullopt| if they do (or there are none).
    std::optional<std::vector<TDiskQuota>> GetUnsatisfiedDiskRequests(TNode* node) const;
};

////////////////////////////////////////////////////////////////////////////////

class TAllocationGroupPlanner
    : public TAllocationGroupPlannerBase
{
public:
    //! NB: Sorts |*availableNodes| in-place.
    TAllocationGroupPlanner(
        TOperationPtr operation,
        std::string allocationGroupName,
        TAllocationGroupResources allocationGroupResources,
        std::vector<TNode*>* availableNodes,
        IAssignmentPlanUpdateContext* context,
        NLogging::TLogger logger,
        bool preemptible = false);

private:
    std::vector<TNode*>* const AvailableNodes_;
    std::vector<TNode*>::iterator NextNodeIt_;
    const bool Preemptible_;

    void AddAssignmentToNode(TNode* node) override;
    TNode* FindBestAvailableNode() override;
};

////////////////////////////////////////////////////////////////////////////////

class TPreemptiveAllocationGroupPlanner
    : public TAllocationGroupPlannerBase
{
public:
    DEFINE_BYVAL_RO_PROPERTY(int, PreemptedAssignmentCount);

public:
    using TBase = TAllocationGroupPlannerBase;

    TPreemptiveAllocationGroupPlanner(
        TOperationPtr operation,
        std::string allocationGroupName,
        TAllocationGroupResources allocationGroupResources,
        std::vector<TNode*>* availableNodes,
        bool useFullHostAggressivePreemption,
        IAssignmentPlanUpdateContext* context,
        TGpuSchedulingPolicyConfigPtr config,
        TInstant now,
        NLogging::TLogger logger);

private:
    const bool UseFullHostAggressivePreemption_;
    const TGpuSchedulingPolicyConfigPtr Config_;
    const TInstant Now_;

    const EAllocationPreemptionReason PreemptionReason_;
    const std::string PreemptionDescription_;

    struct TNodeWithPenalty
    {
        TNode* Node = {};
        NDetail::TPreemptionPenalty Penalty = 0;
    };
    std::vector<TNodeWithPenalty> NodeHeap_;

    struct TNodeState
    {
        TJobResources PreemptibleResourceUsage;
        std::vector<TAssignmentPtr> PreemptibleAssignments;
    };
    THashMap<TNode*, TNodeState> NodeStates_;

    //! Returns the penalty for adding one more assignment to |node|.
    NDetail::TPreemptionPenalty GetNextPreemptionPenaltyForNode(TNode* node) const;

    void AddAssignmentToNode(TNode* node) override;

    TNode* FindBestAvailableNode() override;

    bool ShouldConsiderDiskUsage() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
