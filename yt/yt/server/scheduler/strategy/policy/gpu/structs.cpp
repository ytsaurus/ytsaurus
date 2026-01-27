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
    , ResourceUsage(std::move(resourceUsage))
    , Operation(operation)
    , Node(node)
    , CreationTime(TInstant::Now())
{ }

void Serialize(const TAssignment& assignment, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("node_address").Value(assignment.Node->Address())
            .Item("operation_id").Value(assignment.Operation->GetId())
            .Item("allocation_group_name").Value(assignment.AllocationGroupName)
            .Item("resource_usage").Value(assignment.ResourceUsage.ToJobResources())
            .Item("creation_time").Value(assignment.CreationTime)
            .Item("preemptible").Value(assignment.Preemptible)
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
    YT_VERIFY(!IsInitialized());

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

    EraseOrCrash(Assignments_, assignment);
    AssignedResourceUsage_ -= assignment->ResourceUsage;
    --GetOrCrash(EmptyAssignmentCountPerGroup_, assignment->AllocationGroupName);
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

bool TOperation::IsZeroAssignedUsage() const
{
    return Assignments_.empty();
}

int TOperation::DoGetNeededAllocationCount(const TAllocationGroupResourcesMap& groupedNeededResources) const
{
    int count = 0;
    for (const auto& [_, allocationGroupResources] : groupedNeededResources) {
        count += allocationGroupResources.AllocationCount;
    }

    return count;
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
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TNode::TNode(std::string address)
    : Address_(std::move(address))
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
}

void TNode::PreemptAssignment(const TAssignmentPtr& assignment)
{
    RemoveAssignment(assignment);

    // TODO(yaishenka): Save assignments with allocations.
    // InsertOrCrash(PreemptedAssignments_, assignment);
}

void Serialize(const TNode& node, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("assignments").List(node.Assignments())
            .Item("scheduling_module").Value(node.SchedulingModule())
            .Item("assigned_resource_usage").Value(node.AssignedResourceUsage())
            .DoIf(static_cast<bool>(node.Descriptor()), [&] (auto fluent) {
                fluent
                    .Item("resourse_limits").Value(node.Descriptor()->ResourceLimits)
                    .Item("resourse_usage").Value(node.Descriptor()->ResourceUsage);
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

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
