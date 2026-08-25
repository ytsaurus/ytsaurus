#include "allocation_group_planner.h"

#include "assignment_plan_update.h"
#include "private.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

NDetail::TPreemptionPenalty ComputeAssignmentPreemptionPenalty(
    const TAssignmentPtr& assignment,
    const TGpuSchedulingPolicyConfigPtr& config,
    TInstant now)
{
    auto duration = [&] {
        if (!assignment->PreemptibleProgressStartTime) {
            return config->MinAssignmentPreemptibleDuration;
        }
        return std::max(
            now - *assignment->PreemptibleProgressStartTime,
            config->MinAssignmentPreemptibleDuration);
    }();

    return static_cast<NDetail::TPreemptionPenalty>(duration.Seconds()) *
        assignment->ResourceUsage.GetGpu();
}

////////////////////////////////////////////////////////////////////////////////

TAllocationGroupPlannerBase::TAllocationGroupPlannerBase(
    TOperationPtr operation,
    std::string allocationGroupName,
    TAllocationGroupResources allocationGroupResources,
    IAssignmentPlanUpdateContext* context,
    NLogging::TLogger logger)
    : Operation_(std::move(operation))
    , AllocationGroupName_(std::move(allocationGroupName))
    , AllocationGroupResources_(std::move(allocationGroupResources))
    , Context_(context)
    , Logger(std::move(logger))
{ }

void TAllocationGroupPlannerBase::Run()
{
    while (GetPlannedAssignmentCount() < AllocationGroupResources_.AllocationCount) {
        auto* node = FindBestAvailableNode();
        if (!node) {
            break;
        }

        AddAssignmentToNode(node);
    }
}

int TAllocationGroupPlannerBase::GetPlannedAssignmentCount() const
{
    return std::ssize(PlannedAssignments_);
}

// TODO(eshcherbin): Support genuine disk usage discount.
bool TAllocationGroupPlannerBase::CanAddAssignmentToNode(
    TNode* node,
    const TJobResources& discount) const
{
    const bool isDetailedLoggingEnabled = Context_->IsDetailedLoggingEnabled(Operation_);
    const auto& Logger = this->Logger
        .WithTag("Node", node->Address())
        .WithTag("OperationId", Operation_->GetId())
        .WithTag("AllocationGroup", AllocationGroupName_);

    const auto& nodeTags = node->Descriptor()->Tags;
    if (!Operation_->SchedulingTagFilter().CanSchedule(nodeTags)) {
        YT_TLOG_DEBUG_IF(isDetailedLoggingEnabled, "Cannot add assignment to node: scheduling tag filter mismatch");
        return false;
    }

    // NB(eshcherbin): Check disk request lazily only if resources request can be satisfied.
    if (!CanSatisfyResourceRequest(node, discount)) {
        YT_TLOG_DEBUG_IF(isDetailedLoggingEnabled, "Cannot add assignment to node: insufficient resources")
            .With("ResourceLimits", node->Descriptor()->ResourceLimits)
            .With("RequiredResources", GetRequiredResources(node, discount));
        return false;
    }

    if (auto unsatisfiedDiskRequests = GetUnsatisfiedDiskRequests(node)) {
        YT_TLOG_DEBUG_IF(isDetailedLoggingEnabled, "Cannot add assignment to node: insufficient disk")
            .With("DiskResources", node->Descriptor()->DiskResources)
            .With("DiskRequests", *unsatisfiedDiskRequests);
        return false;
    }

    return true;
}

TJobResources TAllocationGroupPlannerBase::GetRequiredResources(
    TNode* node,
    const TJobResources& discount) const
{
    return (node->AssignedResourceUsage() - discount) + AllocationGroupResources_.MinNeededResources.ToJobResources();
}

bool TAllocationGroupPlannerBase::CanSatisfyResourceRequest(
    TNode* node,
    const TJobResources& discount) const
{
    return Dominates(node->Descriptor()->ResourceLimits, GetRequiredResources(node, discount));
}

std::vector<TDiskQuota> TAllocationGroupPlannerBase::GetDiskRequests(TNode* node) const
{
    std::vector<TDiskQuota> diskRequests;
    if (ShouldConsiderDiskUsage()) {
        diskRequests = node->GetPreliminaryAssignedDiskRequests();
    }
    if (const auto& diskRequest = AllocationGroupResources_.MinNeededResources.DiskQuota()) {
        diskRequests.push_back(diskRequest);
    }

    return diskRequests;
}

std::optional<std::vector<TDiskQuota>> TAllocationGroupPlannerBase::GetUnsatisfiedDiskRequests(TNode* node) const
{
    if (!AllocationGroupResources_.MinNeededResources.DiskQuota()) {
        return std::nullopt;
    }

    auto diskRequests = GetDiskRequests(node);
    if (CanSatisfyDiskQuotaRequests(node->Descriptor()->DiskResources, diskRequests, ShouldConsiderDiskUsage())) {
        return std::nullopt;
    }

    return diskRequests;
}

void TAllocationGroupPlannerBase::AddAssignmentToNode(TNode* node)
{
    PlannedAssignments_.push_back(Context_->AddPlannedAssignment(
        AllocationGroupName_,
        AllocationGroupResources_.MinNeededResources,
        Operation_.Get(),
        node));
}

bool TAllocationGroupPlannerBase::ShouldConsiderDiskUsage() const
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TAllocationGroupPlanner::TAllocationGroupPlanner(
    TOperationPtr operation,
    std::string allocationGroupName,
    TAllocationGroupResources allocationGroupResources,
    std::vector<TNode*>* availableNodes,
    IAssignmentPlanUpdateContext* context,
    NLogging::TLogger logger,
    bool preemptible)
    : TAllocationGroupPlannerBase(
        std::move(operation),
        std::move(allocationGroupName),
        std::move(allocationGroupResources),
        context,
        std::move(logger))
    , AvailableNodes_(availableNodes)
    , Preemptible_(preemptible)
{
    std::ranges::sort(
        *AvailableNodes_,
        [&] (const auto* lhs, const auto* rhs) {
            return lhs->GetUnassignedGpuCount() < rhs->GetUnassignedGpuCount();
        });
    NextNodeIt_ = AvailableNodes_->begin();
}

void TAllocationGroupPlanner::AddAssignmentToNode(TNode* node)
{
    PlannedAssignments_.push_back(Context_->AddPlannedAssignment(
        AllocationGroupName_,
        AllocationGroupResources_.MinNeededResources,
        Operation_.Get(),
        node,
        /*preemptible*/ Preemptible_));
}

TNode* TAllocationGroupPlanner::FindBestAvailableNode()
{
    while (NextNodeIt_ != AvailableNodes_->end()) {
        if (CanAddAssignmentToNode(*NextNodeIt_)) {
            return *NextNodeIt_;
        }

        ++NextNodeIt_;
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

TPreemptiveAllocationGroupPlanner::TPreemptiveAllocationGroupPlanner(
    TOperationPtr operation,
    std::string allocationGroupName,
    TAllocationGroupResources allocationGroupResources,
    std::vector<TNode*>* availableNodes,
    bool useFullHostAggressivePreemption,
    IAssignmentPlanUpdateContext* context,
    TGpuSchedulingPolicyConfigPtr config,
    TInstant now,
    NLogging::TLogger logger)
    : TAllocationGroupPlannerBase(
        std::move(operation),
        std::move(allocationGroupName),
        std::move(allocationGroupResources),
        context,
        std::move(logger))
    , UseFullHostAggressivePreemption_(useFullHostAggressivePreemption)
    , Config_(std::move(config))
    , Now_(now)
    , PreemptionReason_(UseFullHostAggressivePreemption_
        ? EAllocationPreemptionReason::FullHostAggressivePreemption
        : EAllocationPreemptionReason::Preemption)
    , PreemptionDescription_(UseFullHostAggressivePreemption_
        ? Format("Aggressively preempted to plan an assignment for full-host operation %v", Operation_->GetId())
        : Format("Preempted to plan an assignment for operation %v", Operation_->GetId()))
{
    NodeStates_.reserve(availableNodes->size());
    NodeHeap_.reserve(availableNodes->size());
    for (auto* node : *availableNodes) {
        auto& nodeState = NodeStates_[node];
        for (const auto& assignment : node->Assignments()) {
            if (assignment->Reviving) {
                continue;
            }

            bool preemptible = assignment->Preemptible ||
                (UseFullHostAggressivePreemption_ && !assignment->Operation->IsFullHost());
            if (preemptible) {
                nodeState.PreemptibleAssignments.push_back(assignment);
                nodeState.PreemptibleResourceUsage += assignment->ResourceUsage;
            }
        }

        std::ranges::sort(
            nodeState.PreemptibleAssignments,
            /*comp*/ std::greater{},
            /*proj*/ [&] (const auto& assignment) { return ComputeAssignmentPreemptionPenalty(assignment, Config_, Now_); });

        if (CanAddAssignmentToNode(node, /*discount*/ nodeState.PreemptibleResourceUsage)) {
            NodeHeap_.push_back(TNodeWithPenalty{
                .Node = node,
                .Penalty = GetNextPreemptionPenaltyForNode(node),
            });
        }
    }

    std::ranges::make_heap(
        NodeHeap_,
        /*comp*/ std::greater{},
        /*proj*/ [&] (const auto& nodeWithPenalty) { return nodeWithPenalty.Penalty; });
}

// TODO(eshcherbin): Current greedy algorithm is quite naive. We can do much better, maybe even just solve the knapsack problem.
NDetail::TPreemptionPenalty TPreemptiveAllocationGroupPlanner::GetNextPreemptionPenaltyForNode(TNode* node) const
{
    const auto& nodeState = GetOrCrash(NodeStates_, node);
    NDetail::TPreemptionPenalty penalty = 0;
    TJobResources preliminaryPreemptedResources;
    auto it = nodeState.PreemptibleAssignments.rbegin();
    while (!CanAddAssignmentToNode(node, /*discount*/ preliminaryPreemptedResources)) {
        YT_VERIFY(it != nodeState.PreemptibleAssignments.rend());

        const auto& assignment = *it;
        preliminaryPreemptedResources += assignment->ResourceUsage;
        penalty += ComputeAssignmentPreemptionPenalty(assignment, Config_, Now_);
        ++it;
    }

    return penalty;
}

void TPreemptiveAllocationGroupPlanner::AddAssignmentToNode(TNode* node)
{
    auto& nodeState = GetOrCrash(NodeStates_, node);
    while (!CanAddAssignmentToNode(node)) {
        YT_VERIFY(!nodeState.PreemptibleAssignments.empty());

        auto preemptibleAssignment = nodeState.PreemptibleAssignments.back();
        nodeState.PreemptibleAssignments.pop_back();
        nodeState.PreemptibleResourceUsage -= preemptibleAssignment->ResourceUsage;

        Context_->PreemptAssignment(
            preemptibleAssignment,
            PreemptionReason_,
            PreemptionDescription_,
            Operation_->GetId());

        ++PreemptedAssignmentCount_;
    }

    TBase::AddAssignmentToNode(node);

    if (CanAddAssignmentToNode(node, /*discount*/ nodeState.PreemptibleResourceUsage)) {
        NodeHeap_.push_back(TNodeWithPenalty{
            .Node = node,
            .Penalty = GetNextPreemptionPenaltyForNode(node),
        });
        std::ranges::push_heap(
            NodeHeap_,
            /*comp*/ std::greater{},
            /*proj*/ [&] (const auto& nodeWithPenalty) { return nodeWithPenalty.Penalty; });
    }
}

TNode* TPreemptiveAllocationGroupPlanner::FindBestAvailableNode()
{
    if (NodeHeap_.empty()) {
        return {};
    }

    std::ranges::pop_heap(
        NodeHeap_,
        /*comp*/ std::greater{},
        /*proj*/ [&] (const auto& nodeWithPenalty) { return nodeWithPenalty.Penalty; });

    auto* node = NodeHeap_.back().Node;
    NodeHeap_.pop_back();

    return node;
}

bool TPreemptiveAllocationGroupPlanner::ShouldConsiderDiskUsage() const
{
    return !UseFullHostAggressivePreemption_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
