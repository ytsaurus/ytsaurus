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
{ }

////////////////////////////////////////////////////////////////////////////////

TOperation::TOperation(
    TOperationId id,
    EOperationType type,
    bool gang,
    std::optional<THashSet<std::string>> specifiedSchedulingModules)
    : Id_(id)
    , Type_(type)
    , SpecifiedSchedulingModules_(std::move(specifiedSchedulingModules))
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

void TOperation::AddAssignment(const TAssignmentPtr& assignment)
{
    YT_VERIFY(assignment->Operation == this);

    InsertOrCrash(Assignments_, assignment);
    AssignedResourceUsage_ += assignment->ResourceUsage;
    ++AssignmentCountPerGroup_[assignment->AllocationGroupName];

    auto& allocationGroupResources = GetOrCrash(ReadyToAssignGroupedNeededResources_, assignment->AllocationGroupName);
    YT_VERIFY(allocationGroupResources.AllocationCount > 0);
    --allocationGroupResources.AllocationCount;
}

void TOperation::RemoveAssignment(const TAssignmentPtr& assignment)
{
    YT_VERIFY(assignment->Operation == this);

    EraseOrCrash(Assignments_, assignment);
    AssignedResourceUsage_ -= assignment->ResourceUsage;
    --GetOrCrash(AssignmentCountPerGroup_, assignment->AllocationGroupName);
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

int TOperation::DoGetNeededAllocationCount(const TAllocationGroupResourcesMap& groupedNeededResources) const
{
    int count = 0;
    for (const auto& [_, allocationGroupResources] : groupedNeededResources) {
        count += allocationGroupResources.AllocationCount;
    }

    return count;
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
    InsertOrCrash(PreemptedAssignments_, assignment);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
