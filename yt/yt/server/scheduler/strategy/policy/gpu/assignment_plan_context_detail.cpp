#include "assignment_plan_context_detail.h"

#include "private.h"
#include "helpers.h"

#include <yt/yt/server/scheduler/common/public.h>

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TAssignmentPlanContextBase::TAssignmentPlanContextBase(TLogger logger)
    : Logger(std::move(logger))
{ }

void TAssignmentPlanContextBase::AddPlannedAssignment(
    std::string allocationGroupName,
    TJobResourcesWithQuota resourceUsage,
    TOperation* operation,
    TNode* node,
    bool preemptible)
{
    auto assignment = New<TAssignment>(
        std::move(allocationGroupName),
        std::move(resourceUsage),
        operation,
        node);

    assignment->Preemptible = preemptible;

    assignment->Node->AddAssignment(assignment);
    assignment->Operation->AddPlannedAssignment(assignment, preemptible);

    LogStructuredGpuEventFluently(EGpuSchedulingLogEventType::AssignmentAdded)
            .Item("operation_id").Value(operation->GetId())
            .Item("node_address").Value(node->Address())
            .Item("assignment").Value(assignment);

    YT_LOG_DEBUG("Added assignment (AllocationGroupName: %v, ResourceUsage: %v, NodeAddress: %v,  Preemptible: %v, OperationId: %v)",
        assignment->AllocationGroupName,
        assignment->ResourceUsage,
        assignment->Node->Address(),
        assignment->Preemptible,
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

    LogStructuredGpuEventFluently(EGpuSchedulingLogEventType::AssignmentPreempted)
            .Item("assignment").Value(assignment)
            .Item("reason").Value(preemptionReason)
            .Item("description").Value(preemptionDescription);

    YT_LOG_DEBUG(
        "Preempted assignment "
        "(Reason: %v, Description: %v, AllocationGroupName: %v, "
        "ResourceUsage: %v, NodeAddress: %v, OperationId: %v)",
        preemptionReason,
        preemptionDescription,
        assignment->AllocationGroupName,
        assignment->ResourceUsage,
        assignment->Node->Address(),
        assignment->Operation->GetId());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
