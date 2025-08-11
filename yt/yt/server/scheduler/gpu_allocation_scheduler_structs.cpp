#include "gpu_allocation_scheduler_structs.h"

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TGpuSchedulerAssignment::TGpuSchedulerAssignment(
    std::string allocationGroupName,
    TJobResourcesWithQuota resourceUsage,
    TGpuSchedulerOperation* operation,
    TGpuSchedulerNode* node)
    : AllocationGroupName(std::move(allocationGroupName))
    , Operation(operation)
    , Node(node)
    , ResourceUsage(resourceUsage)
{ }

////////////////////////////////////////////////////////////////////////////////

TGpuSchedulerOperation::TGpuSchedulerOperation(
        TOperationId id,
        EOperationType type,
        const TAllocationGroupResourcesMap& initialGroupedNeededResources,
        bool gang,
        std::optional<THashSet<std::string>> specifiedSchedulingModules)
    : Id_(id)
    , Type_(type)
    , InitialGroupedNeededResources_(initialGroupedNeededResources)
    , ReadyToAssignGroupedNeededResources_(initialGroupedNeededResources)
    , SpecifiedSchedulingModules_(std::move(specifiedSchedulingModules))
    , Gang_(gang)
{ }

bool TGpuSchedulerOperation::IsFullHost() const
{
    return std::ranges::all_of(
        InitialGroupedNeededResources_,
        [&] (const auto& pair) {
            const auto& allocationGroupResources = pair.second;
            return allocationGroupResources.MinNeededResources.GetGpu() == MaxNodeGpuCount;
        });
}

bool TGpuSchedulerOperation::IsFullHostModuleBound() const
{
    bool singleAllocationVanilla = GetType() == EOperationType::Vanilla &&
        GetInitialNeededAllocationCount() == 1;
    return IsFullHost() && (IsGang() || singleAllocationVanilla);
}

int TGpuSchedulerOperation::GetInitialNeededAllocationCount() const
{
    return DoGetNeededAllocationCount(InitialGroupedNeededResources_);
}

int TGpuSchedulerOperation::GetReadyToAssignNeededAllocationCount() const
{
    return DoGetNeededAllocationCount(ReadyToAssignGroupedNeededResources_);
}

void TGpuSchedulerOperation::AddAssignment(const TGpuSchedulerAssignmentPtr& assignment)
{
    YT_ASSERT(assignment->Operation == this);

    InsertOrCrash(Assignments_, assignment);
    AssignedResourceUsage_ += assignment->ResourceUsage;

    auto& allocationGroupResources = GetOrCrash(ReadyToAssignGroupedNeededResources_, assignment->AllocationGroupName);
    YT_VERIFY(allocationGroupResources.AllocationCount > 0);
    --allocationGroupResources.AllocationCount;
}

void TGpuSchedulerOperation::RemoveAssignment(const TGpuSchedulerAssignmentPtr& assignment)
{
    YT_ASSERT(assignment->Operation == this);

    EraseOrCrash(Assignments_, assignment);
    AssignedResourceUsage_ -= assignment->ResourceUsage;
}

void TGpuSchedulerOperation::SetPreemptible(bool preemptible)
{
    YT_VERIFY(IsFullHostModuleBound());

    Preemptible_ = preemptible;
    for (const auto& assignment : Assignments_) {
        assignment->Preemptible = preemptible;
    }
}

std::optional<std::string> TGpuSchedulerOperation::GetUsedSchedulingModule() const
{
    if (Assignments_.empty()) {
        return {};
    }

    return (*Assignments_.begin())->Node->SchedulingModule();
}

int TGpuSchedulerOperation::DoGetNeededAllocationCount(const TAllocationGroupResourcesMap& groupedNeededResources) const
{
    int count = 0;
    for (const auto& [_, allocationGroupResources] : groupedNeededResources) {
        count += allocationGroupResources.AllocationCount;
    }

    return count;
}

////////////////////////////////////////////////////////////////////////////////

int TGpuSchedulerNode::GetUnassignedGpuCount() const
{
    return Descriptor_->ResourceLimits.GetGpu() - AssignedResourceUsage_.GetGpu();
}

void TGpuSchedulerNode::SetSchedulingModule(std::string schedulingModule)
{
    YT_VERIFY(!SchedulingModule_);

    SchedulingModule_ = std::move(schedulingModule);
}

void TGpuSchedulerNode::UpdateDescriptor(TExecNodeDescriptorPtr descriptor)
{
    if (Descriptor_) {
        YT_VERIFY(Descriptor_->Id == descriptor->Id);
    }

    Descriptor_ = std::move(descriptor);
}

void TGpuSchedulerNode::AddAssignment(const TGpuSchedulerAssignmentPtr& assignment)
{
    YT_ASSERT(assignment->Node == this);

    InsertOrCrash(Assignments_, assignment);
    AssignedResourceUsage_ += assignment->ResourceUsage;
}

void TGpuSchedulerNode::RemoveAssignment(const TGpuSchedulerAssignmentPtr& assignment)
{
    YT_ASSERT(assignment->Node == this);

    EraseOrCrash(Assignments_, assignment);
    AssignedResourceUsage_ -= assignment->ResourceUsage;
}

void TGpuSchedulerNode::PreemptAssignment(const TGpuSchedulerAssignmentPtr& assignment)
{
    RemoveAssignment(assignment);

    assignment->Preempted = true;
    InsertOrCrash(PreemptedAssignments_, assignment);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
