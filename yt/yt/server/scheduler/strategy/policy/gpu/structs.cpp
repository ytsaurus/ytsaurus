#include "structs.h"

#include "private.h"
#include "helpers.h"

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

TAssignment::TAssignment(
    std::string allocationGroupName,
    TJobResourcesWithQuota resourceUsage,
    TOperation* operation,
    TNode* node)
    : AllocationGroupName(std::move(allocationGroupName))
    , Operation(operation)
    , Node(node)
    , OperationId(operation->GetId())
    , CreationTime(TInstant::Now())
    , ResourceUsage(std::move(resourceUsage))
{ }

void TAssignment::AddAllocation(const TAllocationStatePtr& allocation)
{
    YT_VERIFY(!AllocationId);

    YT_VERIFY(allocation->Assignment().Lock() == this);

    AllocationId = allocation->GetId();

    Operation->AddAllocation(allocation, this);
    Node->AddAllocation(allocation, this);
}

TJobResources TAssignment::UpdateResourceUsage(const TJobResources& newUsage)
{
    auto delta = newUsage - ResourceUsage.ToJobResources();

    ResourceUsage.SetJobResources(newUsage);

    YT_VERIFY(Dominates(ResourceUsage.ToJobResources(), TJobResources()));

    Operation->AssignedResourceUsage_ += delta;
    Node->AssignedResourceUsage_ += delta;

    YT_VERIFY(Dominates(Operation->AssignedResourceUsage_, TJobResources()));
    YT_VERIFY(Dominates(Node->AssignedResourceUsage_, TJobResources()));

    return delta;
}

void Serialize(const TAssignment& assignment, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("node_address").Value(assignment.Node->Address())
            .Item("operation_id").Value(assignment.OperationId)
            .Item("allocation_group_name").Value(assignment.AllocationGroupName)
            .Item("resource_usage").Value(assignment.ResourceUsage.ToJobResources())
            .Item("creation_time").Value(assignment.CreationTime)
            .Item("preemptible").Value(assignment.Preemptible)
            .Item("allocation_id").Value(assignment.AllocationId)
            .Item("preemptible_progress_start_time").Value(assignment.PreemptibleProgressStartTime)
            .Item("reviving").Value(assignment.Reviving)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TPreemptionInfo& preemptionInfo, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("reason").Value(preemptionInfo.Reason)
            .Item("description").Value(preemptionInfo.Description)
            .Item("preempted_for_operation_id").Value(preemptionInfo.PreemptedForOperationId)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TAllocationState::TAllocationState(
    TAllocationId id,
    NNodeTrackerClient::TNodeId nodeId,
    TWeakPtr<TAssignment> assignment,
    const TJobResources& resourceUsage)
    : Id_(id)
    , NodeId_(nodeId)
    , Assignment_(std::move(assignment))
    , ResourceUsage_(resourceUsage)
    , CreationTime_(TInstant::Now())
{ }

TJobResources TAllocationState::UpdateResourceUsage(const TJobResources& newUsage)
{
    auto delta = newUsage - ResourceUsage_;
    ResourceUsage_ = newUsage;

    YT_VERIFY(Dominates(ResourceUsage_, TJobResources()));

    if (auto assignment = Assignment_.Lock()) {
        Y_UNUSED(assignment->UpdateResourceUsage(newUsage));
    }

    return delta;
}

void TAllocationState::SetAssignment(TWeakPtr<TAssignment> assignment)
{
    YT_VERIFY(Assignment_ == nullptr);

    Assignment_ = std::move(assignment);
}

TAllocationSnapshotState TAllocationState::BuildSnapshotInfo(TOperationId operationId) const
{
    TAllocationSnapshotState info{
        .AllocationId = Id_,
        .OperationId = operationId,
        .NodeId = NodeId_,
        .ResourceUsage = ResourceUsage_,
    };
    if (auto assignment = Assignment_.Lock()) {
        info.Preemptible = assignment->Preemptible;
    }
    return info;
}

void Serialize(const TAllocationState& allocation, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("allocation_id").Value(allocation.GetId())
            .Item("resource_usage").Value(allocation.ResourceUsage())
            .OptionalItem("preemption_info", allocation.PreemptionInfo())
            .Item("creation_time").Value(allocation.GetCreationTime())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TOperation::TOperation(
    TOperationId id,
    EOperationType type,
    bool gang,
    std::optional<THashSet<std::string>> specifiedSchedulingModules,
    TSchedulingTagFilter schedulingTagFilter)
    : Id_(id)
    , Type_(type)
    , SpecifiedSchedulingModules_(std::move(specifiedSchedulingModules))
    , SchedulingTagFilter_(std::move(schedulingTagFilter))
    , Gang_(gang)
{ }

void TOperation::Initialize(const TAllocationGroupResourcesMap& initialGroupedNeededResources)
{
    if (InitialGroupedNeededResources_.has_value()) {
        YT_VERIFY(InitialGroupedNeededResources_ == initialGroupedNeededResources);
    }

    InitialGroupedNeededResources_ = initialGroupedNeededResources;
}

bool TOperation::IsInitialized() const
{
    return InitialGroupedNeededResources_.has_value();
}

bool TOperation::IsFullHost() const
{
    return std::ranges::all_of(
        *InitialGroupedNeededResources_,
        [&] (const auto& pair) {
            const auto& allocationGroupResources = pair.second;
            return allocationGroupResources.MinNeededResources.GetGpu() == MaxNodeGpuCount;
        });
}

bool TOperation::IsFullHostModuleBound() const
{
    bool singleAllocationVanilla = GetType() == EOperationType::Vanilla &&
        GetInitialNeededAllocationCount() == 1;
    return IsFullHost() && (IsGang() || singleAllocationVanilla);
}

int TOperation::GetInitialNeededAllocationCount() const
{
    return DoGetNeededAllocationCount(*InitialGroupedNeededResources_);
}

int TOperation::GetReadyToAssignNeededAllocationCount() const
{
    return DoGetNeededAllocationCount(ReadyToAssignGroupedNeededResources_);
}

int TOperation::GetExtraNeededAllocationCount() const
{
    return DoGetNeededAllocationCount(ExtraGroupedNeededResources_);
}

void TOperation::AddPlannedAssignment(const TAssignmentPtr& assignment, bool withExtraResources)
{
    YT_VERIFY(assignment->Operation == this);

    AddAssignment(assignment);

    auto& allocationGroupResources = withExtraResources
        ? GetOrCrash(ExtraGroupedNeededResources_, assignment->AllocationGroupName)
        : GetOrCrash(ReadyToAssignGroupedNeededResources_, assignment->AllocationGroupName);

    YT_VERIFY(allocationGroupResources.AllocationCount > 0);
    --allocationGroupResources.AllocationCount;
}

void TOperation::RemoveAssignment(const TAssignmentPtr& assignment)
{
    YT_VERIFY(assignment->Operation == this);

    AssignedResourceUsage_ -= assignment->ResourceUsage;

    if (assignment->AllocationId) {
        EraseOrCrash(AllocationIdToAssignment_, assignment->AllocationId);
    } else {
        auto it = GetIteratorOrCrash(EmptyAssignmentCountPerGroup_, assignment->AllocationGroupName);
        YT_VERIFY(it->second > 0);
        --it->second;
    }

    EraseOrCrash(Assignments_, assignment);
}

void TOperation::AddAssignment(const TAssignmentPtr& assignment)
{
    YT_VERIFY(assignment->Operation == this);

    InsertOrCrash(Assignments_, assignment);
    AssignedResourceUsage_ += assignment->ResourceUsage;
    ++EmptyAssignmentCountPerGroup_[assignment->AllocationGroupName];
}

void TOperation::SetPreemptible(bool preemptible)
{
    YT_VERIFY(IsFullHostModuleBound());

    Preemptible_ = preemptible;
    for (const auto& assignment : Assignments_) {
        assignment->Preemptible = preemptible;
    }
}

std::optional<std::string> TOperation::GetUsedSchedulingModule() const
{
    if (Assignments_.empty()) {
        return {};
    }

    return (*Assignments_.begin())->Node->SchedulingModule();
}

void TOperation::ResetSchedulingModule()
{
    SchedulingModule_.reset();
    NetworkPriority_.reset();
}

bool TOperation::IsZeroAssignedUsage() const
{
    return Assignments_.empty();
}

void TOperation::AddAllocation(const TAllocationStatePtr& allocation, const TAssignmentPtr& assignment)
{
    YT_VERIFY(Assignments_.contains(assignment));
    YT_VERIFY(GetOrCrash(EmptyAssignmentCountPerGroup_, assignment->AllocationGroupName) > 0);

    auto allocationId = allocation->GetId();

    --GetOrCrash(EmptyAssignmentCountPerGroup_, assignment->AllocationGroupName);
    EmplaceOrCrash(AllocationIdToAssignment_, allocationId, assignment);

    auto [it, inserted] = AllocationIdToAllocationState_.try_emplace(allocationId, allocation);
    YT_VERIFY(inserted || it->second == allocation);
}

void TOperation::AddOrphanAllocation(const TAllocationStatePtr& allocation)
{
    YT_VERIFY(allocation->Assignment() == nullptr);
    EmplaceOrCrash(AllocationIdToAllocationState_, allocation->GetId(), allocation);
}

void TOperation::AddRevivedAllocation(
    const TAllocationStatePtr& allocation,
    const TAssignmentPtr& assignment)
{
    YT_VERIFY(Assignments_.contains(assignment));
    YT_VERIFY(assignment->AllocationId == allocation->GetId());
    YT_VERIFY(GetOrCrash(AllocationIdToAssignment_, assignment->AllocationId) == assignment);
    EmplaceOrCrash(AllocationIdToAllocationState_, allocation->GetId(), allocation);
}

void TOperation::RemoveAllocation(TAllocationId allocationId)
{
    EraseOrCrash(AllocationIdToAllocationState_, allocationId);
}

void TOperation::RemoveAllAllocations()
{
    AllocationIdToAllocationState_.clear();
}

int TOperation::DoGetNeededAllocationCount(const TAllocationGroupResourcesMap& groupedNeededResources) const
{
    int count = 0;
    for (const auto& [_, allocationGroupResources] : groupedNeededResources) {
        count += allocationGroupResources.AllocationCount;
    }

    return count;
}

TOperationSnapshotState TOperation::BuildSnapshotInfo() const
{
    TOperationSnapshotState info{
        .Preemptible = Preemptible_,
        .Starving = Starving_,
        .Enabled = Enabled_,
        .SchedulingModule = SchedulingModule_,
    };

    for (const auto& assignment : Assignments_) {
        if (IsAssignmentPreliminary(assignment)) {
            ++info.PreliminaryAssignmentCount;
        } else {
            ++info.RealizedAssignmentCount;
        }
    }

    info.AllocationIds.reserve(AllocationIdToAllocationState_.size());
    for (const auto& [allocationId, _] : AllocationIdToAllocationState_) {
        info.AllocationIds.push_back(allocationId);
    }

    return info;
}

void Serialize(const TOperation& operation, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("assignments").List(operation.Assignments())
            .Item("type").Value(operation.GetType())
            .Item("enabled").Value(operation.IsEnabled())
            .Item("gang").Value(operation.IsGang())
            .Item("initial_grouped_needed_resources").Value(operation.InitialGroupedNeededResources())
            .Item("assigned_resource_usage").Value(operation.AssignedResourceUsage())
            .Item("specified_scheduling_modules").Value(operation.SpecifiedSchedulingModules())
            .Item("priority_module_binding_enabled").Value(operation.IsPriorityModuleBindingEnabled())
            .Item("waiting_for_module_binding_since").Value(operation.WaitingForModuleBindingSince())
            .Item("waiting_for_assignments_since").Value(operation.WaitingForAssignmentsSince())
            .Item("preemptible").Value(operation.IsPreemptible())
            .Item("starving").Value(operation.IsStarving())
            .Item("scheduling_module").Value(operation.SchedulingModule())
            .OptionalItem("network_priority", operation.NetworkPriority())
            // TODO(yaishenka): Think about what else to expose here.
            .Item("allocations").Value(operation.AllocationIdToAllocationState())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TNode::TNode(NNodeTrackerClient::TNodeId id, std::string address)
    : Id_(id)
    , Address_(std::move(address))
{ }

bool TNode::IsSchedulable() const
{
    return Descriptor_ &&
        Descriptor_->Online &&
        Descriptor_->ResourceLimits.GetUserSlots() > 0 &&
        SchedulingModule_;
}

int TNode::GetUnassignedGpuCount() const
{
    return Descriptor_->ResourceLimits.GetGpu() - AssignedResourceUsage_.GetGpu();
}

void TNode::SetDescriptor(TExecNodeDescriptorPtr descriptor)
{
    if (Descriptor_) {
        YT_VERIFY(Descriptor_->Id == descriptor->Id);
    }

    Descriptor_ = std::move(descriptor);
}

std::vector<TDiskQuota> TNode::GetPreliminaryAssignedDiskRequests() const
{
    std::vector<TDiskQuota> result;
    result.reserve(size(Assignments_));
    for (const auto& assignment : Assignments_) {
        const auto& diskRequest = assignment->ResourceUsage.DiskQuota();
        if (IsAssignmentPreliminary(assignment) && diskRequest) {
            result.push_back(diskRequest);
        }
    }

    return result;
}

void TNode::AddAssignment(const TAssignmentPtr& assignment)
{
    YT_VERIFY(assignment->Node == this);

    InsertOrCrash(Assignments_, assignment);
    AssignedResourceUsage_ += assignment->ResourceUsage;
}

void TNode::RemoveAssignment(const TAssignmentPtr& assignment)
{
    YT_VERIFY(assignment->Node == this);

    EraseOrCrash(Assignments_, assignment);
    AssignedResourceUsage_ -= assignment->ResourceUsage;

    if (assignment->AllocationId) {
        EraseOrCrash(AllocationIdToAssignment_, assignment->AllocationId);
    }
}

void TNode::PreemptAssignment(
    const TAssignmentPtr& assignment,
    EAllocationPreemptionReason reason,
    std::string description,
    TOperationId preemptedForOperationId)
{
    if (assignment->AllocationId) {
        const auto& allocation = GetOrCrash(
            assignment->Operation->AllocationIdToAllocationState(),
            assignment->AllocationId);
        allocation->PreemptionInfo().emplace(TPreemptionInfo{
            .Reason = reason,
            .Description = std::move(description),
            .PreemptedForOperationId = preemptedForOperationId,
        });

        InsertOrCrash(AllocationsToPreempt_, assignment->AllocationId);
    }

    RemoveAssignment(assignment);
}

void TNode::PreemptAllocation(TAllocationId allocationId)
{
    EraseOrCrash(AllocationsToPreempt_, allocationId);
    InsertOrCrash(PreemptedAllocations_, allocationId);
}

void TNode::RemovePreemptedAllocation(TAllocationId allocationId)
{
    EraseOrCrash(PreemptedAllocations_, allocationId);
}

void TNode::AddAllocation(const TAllocationStatePtr& allocation, const TAssignmentPtr& assignment)
{
    YT_VERIFY(Assignments_.contains(assignment));

    EmplaceOrCrash(AllocationIdToAssignment_, allocation->GetId(), assignment);
}

TNodeSnapshotState TNode::BuildSnapshotInfo() const
{
    TNodeSnapshotState info{
        .SchedulingModule = SchedulingModule_,
        .AssignedResourceUsage = AssignedResourceUsage_,
        .AllocationsToPreemptCount = static_cast<int>(std::ssize(AllocationsToPreempt_)),
        .PreemptedAllocationsCount = static_cast<int>(std::ssize(PreemptedAllocations_)),
    };

    info.AllocationIds.reserve(AllocationIdToAssignment_.size());
    for (const auto& [allocationId, _] : AllocationIdToAssignment_) {
        info.AllocationIds.push_back(allocationId);
    }

    return info;
}

void Serialize(const TNode& node, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("assignments").List(node.Assignments())
            .Item("scheduling_module").Value(node.SchedulingModule())
            .Item("assigned_resource_usage").Value(node.AssignedResourceUsage())
            .OptionalItem("last_heartbeat_statistics", node.LastSchedulingHeartbeatStatistics())
            .DoIf(static_cast<bool>(node.Descriptor()), [&] (auto fluent) {
                fluent
                    .Item("resource_limits").Value(node.Descriptor()->ResourceLimits)
                    .Item("resource_usage").Value(node.Descriptor()->ResourceUsage);
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TGpuModuleStatistics& statistic, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("node_count").Value(statistic.TotalNodes)
            .Item("unreserved_node_count").Value(statistic.UnreservedNodes)
            .Item("full_host_bound_operation_count").Value(statistic.FullHostModuleBoundOperations)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TGpuScheduleAllocationsStatisticsPtr& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Do(std::bind(&BuildScheduleAllocationsStatisticsCommon, statistics, std::placeholders::_1))
            .Item("scheduled_allocation_count").Value(statistics->ScheduledAllocationCount)
            .Item("preempted_allocation_count").Value(statistics->PreemptedAllocationCount)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
