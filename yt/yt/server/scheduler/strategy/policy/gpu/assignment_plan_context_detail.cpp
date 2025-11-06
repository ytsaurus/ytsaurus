#include "assignment_plan_context_detail.h"

#include "private.h"

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TAssignmentPlanContextBase::TAssignmentPlanContextBase(TLogger logger)
    : Logger(std::move(logger))
{ }

void TAssignmentPlanContextBase::AddAssignment(
    std::string allocationGroupName,
    TJobResourcesWithQuota resourceUsage,
    TOperation* operation,
    TNode* node)
{
    auto assignment = New<TAssignment>(
        std::move(allocationGroupName),
        std::move(resourceUsage),
        operation,
        node);

    assignment->Node->AddAssignment(assignment);
    assignment->Operation->AddAssignment(assignment);

    YT_LOG_DEBUG("Added assignment (AllocationGroupName: %v, ResourceUsage: %v, NodeAddress: %v, OperationId: %v)",
        assignment->AllocationGroupName,
        assignment->ResourceUsage,
        assignment->Node->Descriptor()->GetDefaultAddress(),
        assignment->Operation->GetId());
}

void TAssignmentPlanContextBase::PreemptAssignment(
    const TAssignmentPtr& assignment,
    EAllocationPreemptionReason preemptionReason,
    std::string preemptionDescription)
{
    assignment->Preempted = true;
    assignment->PreemptionReason = preemptionReason;
    assignment->PreemptionDescription = std::move(preemptionDescription);
    assignment->Node->PreemptAssignment(assignment);
    assignment->Operation->RemoveAssignment(assignment);

    YT_LOG_DEBUG(
        "Preempted assignment "
        "(Reason: %v, Description: %v, AllocationGroupName: %v, "
        "ResourceUsage: %v, NodeAddress: %v, OperationId: %v)",
        preemptionReason,
        preemptionDescription,
        assignment->AllocationGroupName,
        assignment->ResourceUsage,
        assignment->Node->Descriptor()->GetDefaultAddress(),
        assignment->Operation->GetId());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
