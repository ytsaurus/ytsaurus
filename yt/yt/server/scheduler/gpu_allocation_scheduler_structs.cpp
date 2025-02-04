#include "gpu_allocation_scheduler_structs.h"

#include <functional>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TGpuSchedulerOperationState::TGpuSchedulerOperationState(
    TOperationId operationId,
    bool isGang,
    std::optional<THashSet<TString>> specifiedSchedulingModules,
    std::optional<TOperationRuntimeAttributes> runtimeAttributes)
    : OperationId_(operationId)
    , IsGang_(isGang)
    , SpecifiedSchedulingModules_(std::move(specifiedSchedulingModules))
    , RuntimeAttributes_(std::move(runtimeAttributes))
{ }

double TGpuSchedulerOperationState::GetNeededResources() const
{
    if (!RuntimeAttributes_) {
        return 0.0;
    }

    return RuntimeAttributes_->FairResourceAmount - ResourceUsage_;
}

void TGpuSchedulerOperationState::OnAllocationScheduled(const TGpuAllocationStatePtr& allocation, NNodeTrackerClient::TNodeId nodeId)
{
    AllocationsToSchedule_.erase(allocation);

    allocation->Scheduled = true;

    auto resources = allocation->Resources.GetGpu();
    ResourceUsage_ += resources;
    TotalResourceUsage_ += resources;

    EmplaceOrCrash(ScheduledAllocations_, allocation, nodeId);
}

void TGpuSchedulerOperationState::RemoveAllocation(const TGpuAllocationStatePtr& allocation)
{
    ResourceUsage_ -= allocation->Resources.GetGpu();

    EraseOrCrash(ScheduledAllocations_, allocation);
}

void TGpuSchedulerOperationState::ResetUsage()
{
    ResourceUsage_ = 0.0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
