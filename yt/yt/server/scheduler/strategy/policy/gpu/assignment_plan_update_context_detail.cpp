#include "assignment_plan_update_context_detail.h"

#include "helpers.h"

#include <yt/yt/server/scheduler/strategy/pool_tree_snapshot.h>

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TAssignmentHandler::TAssignmentHandler(TLogger logger)
    : Logger(std::move(logger))
{ }

void TAssignmentHandler::AddPlannedAssignment(
    std::string allocationGroupName,
    TJobResourcesWithQuota resourceUsage,
    TOperation* operation,
    TNode* node,
    bool preemptible) const
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

    YT_LOG_DEBUG("Added assignment (AllocationGroupName: %v, ResourceUsage: %v, NodeAddress: %v, Preemptible: %v, OperationId: %v)",
        assignment->AllocationGroupName,
        assignment->ResourceUsage,
        assignment->Node->Address(),
        assignment->Preemptible,
        assignment->Operation->GetId());
}

void TAssignmentHandler::PreemptAssignment(
    const TAssignmentPtr& assignment,
    EAllocationPreemptionReason preemptionReason,
    std::string preemptionDescription) const
{
    assignment->Preempted = true;
    assignment->PreemptionReason = preemptionReason;
    assignment->PreemptionDescription = std::move(preemptionDescription);
    assignment->Node->PreemptAssignment(assignment);
    assignment->Operation->RemoveAssignment(assignment);

    LogStructuredGpuEventFluently(EGpuSchedulingLogEventType::AssignmentPreempted)
        .Item("assignment").Value(assignment)
        .Item("reason").Value(preemptionReason)
        .Item("description").Value(assignment->PreemptionDescription);

    YT_LOG_DEBUG(
        "Preempted assignment "
        "(Reason: %v, Description: %v, AllocationGroupName: %v, "
        "ResourceUsage: %v, NodeAddress: %v, OperationId: %v)",
        assignment->PreemptionReason,
        assignment->PreemptionDescription,
        assignment->AllocationGroupName,
        assignment->ResourceUsage,
        assignment->Node->Address(),
        assignment->Operation->GetId());
}

////////////////////////////////////////////////////////////////////////////////

TAssignmentPlanUpdateContext::TAssignmentPlanUpdateContext(
    NLogging::TLogger logger,
    const TOperationMap& operations,
    const TNodeMap& nodes,
    const TPoolTreeSnapshotPtr& treeSnapshot,
    const TAssignmentHandler& assignmentHandler)
    : Logger(logger)
    , Operations_(operations)
    , Nodes_(nodes)
    , Statistics_(New<TGpuPlanUpdateStatistics>())
    , TreeSnapshot_(treeSnapshot)
    , AssignmentHandler_(assignmentHandler)
    , AttributesList_(TreeSnapshot_->RootElement()->GetTreeSize())
{ }

const TOperationMap& TAssignmentPlanUpdateContext::Operations() const
{
    return Operations_;
}

const TNodeMap& TAssignmentPlanUpdateContext::Nodes() const
{
    return Nodes_;
}

const TGpuPlanUpdateStatisticsPtr& TAssignmentPlanUpdateContext::GetStatistics() const
{
    return Statistics_;
}

void TAssignmentPlanUpdateContext::AddPlannedAssignment(
    std::string allocationGroupName,
    TJobResourcesWithQuota resourceUsage,
    TOperation* operation,
    TNode* node,
    bool preemptible)
{
    IncreaseOperationUsage(operation, resourceUsage);
    AssignmentHandler_.AddPlannedAssignment(std::move(allocationGroupName), resourceUsage, operation, node, preemptible);
}

void TAssignmentPlanUpdateContext::PreemptAssignment(
    const TAssignmentPtr& assignment,
    EAllocationPreemptionReason preemptionReason,
    std::string preemptionDescription)
{
    IncreaseOperationUsage(assignment->Operation, -assignment->ResourceUsage);
    AssignmentHandler_.PreemptAssignment(assignment, preemptionReason, std::move(preemptionDescription));
}

TJobResources TAssignmentPlanUpdateContext::GetAvailableOperationLimits(const TOperationPtr& operation) const
{
    const TPoolTreeElement* treeElement = GetOperationElement(operation);
    YT_VERIFY(treeElement);

    auto availableLimits = TJobResources::Infinite();
    while (treeElement) {
        auto usage = AttributesList_.AttributesOf(treeElement).AssignedResourceUsage;
        if (treeElement->MaybeSpecifiedResourceLimits()) {
            availableLimits = Min(availableLimits, treeElement->ResourceLimits() - usage);
        }
        treeElement = treeElement->GetParent();
    }
    return availableLimits;
}

std::optional<TString> TAssignmentPlanUpdateContext::FindLimitViolatingParentId(const TPoolTreeElement* element) const
{
    while (element) {
        auto usage = AttributesList_.AttributesOf(element).AssignedResourceUsage;
        if (element->MaybeSpecifiedResourceLimits()) {
            if (!Dominates(element->ResourceLimits(), usage)) {
                return element->GetId();
            }
        }
        element = element->GetParent();
    }
    return {};
}

void TAssignmentPlanUpdateContext::UpdatePreemptionStatuses() const
{
    for (const auto& [_, operation] : Operations_) {
        YT_VERIFY(operation->IsInitialized());

        const auto* operationElement = GetOperationElement(operation);
        if (!operationElement) {
            YT_LOG_DEBUG("Operation is not present in the tree snapshot (OperationId: %v, TreeSnapshotId: %v)",
                operation->GetId(),
                TreeSnapshot_->GetId());

            ResetOperationResources(operation);
            continue;
        }

        UpdatePreemptionStatus(operation, operationElement);
    }
}

void TAssignmentPlanUpdateContext::FillOperationUsage()
{
    for (const auto& [_, operation] : Operations_) {
        if (!GetOperationElement(operation)) {
            continue;
        }

        IncreaseOperationUsage(operation, operation->AssignedResourceUsage());
    }
}

void TAssignmentPlanUpdateContext::PreemptLimitViolatingOperations()
{
    for (const auto& [_, operation] : Operations_) {
        auto element = GetOperationElement(operation);
        if (!element) {
            continue;
        }

        // NB(severovv): Copy assignments with |GetItems|, because the set will be modified.
        for (const auto& assignment : GetItems(operation->Assignments())) {
            if (!assignment->Preemptible) {
                continue;
            }

            auto violatedId = FindLimitViolatingParentId(element);
            if (!violatedId) {
                break;
            }

            std::string preemptionDescription;
            if (violatedId.value() == element->GetId()) {
                preemptionDescription = Format("Preempted due to violation of resource limits of operation %v", violatedId);
            } else {
                preemptionDescription = Format("Preempted due to violation of limits on pool %Qv", violatedId);
            }

            Statistics_->PreemptedAssignments++;
            PreemptAssignment(assignment, EAllocationPreemptionReason::ResourceLimitsViolated, preemptionDescription);
        }
    }
}

// TODO(eshcherbin): Optimize not to recalculate preemptible assignments and ready to assign resources from scratch.
void TAssignmentPlanUpdateContext::UpdateOperationResources(const TOperationPtr& operation) const
{
    YT_VERIFY(operation->IsInitialized());

    // Skip disabled operation.
    const auto* operationElement = GetOperationElement(operation);
    if (!operationElement) {
        return;
    }

    auto convertToShare = [&] (const TJobResources& allocationResources) -> TResourceVector {
        return TResourceVector::FromJobResources(allocationResources, operationElement->GetTotalResourceLimits());
    };

    const auto& fairShare = operationElement->Attributes().FairShare.Total;

    // Update ready to assign resources.
    const auto assignedUsageShare = convertToShare(operation->AssignedResourceUsage());
    TResourceVector readyToAssignShare;
    operation->ReadyToAssignGroupedNeededResources().clear();

    TResourceVector extraShare;
    operation->ExtraGroupedNeededResources().clear();

    // Preemptible FHMB operations do not deserve resources.
    if (operation->IsFullHostModuleBound() && operation->IsPreemptible()) {
        YT_LOG_DEBUG("Skipping FHMB operation because it is preemptible (OperationId: %v, FairShare: %v)", operation->GetId(), fairShare);
        return;
    }

    // For an operation we want to maintain the following invariant:
    //     ResourceUsage + EmptyAssignmentResources + ReadyToAssignResources ~= FairShare.
    // Note that ResourceUsage + EmptyAssignmentResources == AssignedResourceUsage.
    for (const auto& [neededAllocationGroupName, neededAllocationGroupResources] : GetGroupedNeededResources(operation, operationElement)) {
        auto readyToAssignIt = EmplaceOrCrash(
            operation->ReadyToAssignGroupedNeededResources(),
            neededAllocationGroupName,
            TAllocationGroupResources{.MinNeededResources = neededAllocationGroupResources.MinNeededResources});
        auto& readyToAssignResources = readyToAssignIt->second;

        auto extraIt = operation->ExtraGroupedNeededResources().emplace(
            neededAllocationGroupName,
            TAllocationGroupResources{.MinNeededResources = neededAllocationGroupResources.MinNeededResources})
            .first;
        auto& extraResources = extraIt->second;

        const auto allocationUsageShare = convertToShare(neededAllocationGroupResources.MinNeededResources);
        const auto emptyAssignmentCount = GetOrDefault(operation->EmptyAssignmentCountPerGroup(), neededAllocationGroupName);

        YT_LOG_DEBUG(
            "Updating operation resources for allocation group "
            "(OperationId: %v, AllocationGroup: %v, NeededAllocationCount: %v, MinNeededResources: %v, "
            "EmptyAssignmentCount: %v, FairShare: %v, AllocationUsageShare: %v)",
            operation->GetId(),
            neededAllocationGroupName,
            neededAllocationGroupResources.AllocationCount,
            neededAllocationGroupResources.MinNeededResources,
            emptyAssignmentCount,
            fairShare,
            allocationUsageShare);

        while (emptyAssignmentCount + readyToAssignResources.AllocationCount + extraResources.AllocationCount < neededAllocationGroupResources.AllocationCount) {
            auto sumOfUsageShare = assignedUsageShare + readyToAssignShare + extraShare + allocationUsageShare;
            bool belowFairShare = Dominates(fairShare + TResourceVector::Epsilon(), sumOfUsageShare);

            YT_LOG_DEBUG_UNLESS(
                !operationElement->AreDetailedLogsEnabled(),
                "Checking if fair share is exceeded before adding another assignment "
                "(OperationId: %v, AllocationGroup: %v, AssignedUsageShare: %v, "
                "ReadyToAssignShare: %v, FairShare: %v, ExtraShare: %v, SumOfUsageShare: %v, BelowFairShare: %v)",
                operation->GetId(),
                neededAllocationGroupName,
                assignedUsageShare,
                readyToAssignShare,
                fairShare,
                extraShare,
                sumOfUsageShare,
                belowFairShare);

            if (belowFairShare) {
                ++readyToAssignResources.AllocationCount;
                readyToAssignShare += allocationUsageShare;
            } else {
                if (operation->IsFullHostModuleBound()) {
                    break;
                }
                ++extraResources.AllocationCount;
                extraShare += allocationUsageShare;
            }
        }
    }
}

void TAssignmentPlanUpdateContext::ResetOperationResources(const TOperationPtr& operation) const
{
    if (!operation->IsInitialized()) {
        return;
    }

    operation->ReadyToAssignGroupedNeededResources().clear();

    if (operation->IsFullHostModuleBound()) {
        operation->SetPreemptible(true);
    } else {
        for (const auto& assignment : operation->Assignments()) {
            assignment->Preemptible = true;
        }
    }
}

TPoolTreeOperationElement* TAssignmentPlanUpdateContext::GetOperationElement(const TOperationPtr& operation) const
{
    return TreeSnapshot_->FindEnabledOperationElement(operation->GetId());
}

// TODO(eshcherbin): Optimize not to recalculate preemptible assignments and ready to assign resources from scratch.
void TAssignmentPlanUpdateContext::UpdatePreemptionStatus(const TOperationPtr& operation, const TPoolTreeOperationElement* operationElement) const
{
    operation->SetStarving(operationElement->GetStarvationStatus() != EStarvationStatus::NonStarving);
    if (operation->IsStarving()) {
        YT_LOG_DEBUG("Operation is starving (OperationId: %v)", operation->GetId());
    }

    auto convertToShare = [&] (const TJobResources& allocationResources) -> TResourceVector {
        return TResourceVector::FromJobResources(allocationResources, operationElement->GetTotalResourceLimits());
    };

    const auto& fairShare = operationElement->Attributes().FairShare.Total;

    // Update preemptible allocations.
    if (operation->IsFullHostModuleBound()) {
        operation->SetPreemptible(Dominates(TResourceVector::Epsilon(), fairShare));
    } else {
        auto sortedAssignments = GetItems(operation->Assignments());
        // TODO(eshcherbin): Sort assignments by allocation start time.
        std::ranges::sort(sortedAssignments, std::less<>(), [] (const TAssignmentPtr& assignment) {
            return assignment->AllocationGroupName;
        });

        TResourceVector usageShare;
        for (const auto& assignment : sortedAssignments) {
            // NB(yaishenka): Assignment is preemptible if total resource usage (including current assignment) is higher than fair share.
            usageShare += convertToShare(assignment->ResourceUsage);
            bool previousStatus = std::exchange(
                assignment->Preemptible,
                !Dominates(fairShare + TResourceVector::Epsilon(), usageShare));

            if (previousStatus != assignment->Preemptible) {
                YT_LOG_DEBUG("Changed assignment preemptible status (OperationId: %v, Preemptible: %v, FairShare: %v, UsageShare: %v)",
                    operation->GetId(),
                    assignment->Preemptible,
                    fairShare,
                    usageShare);
            }
        }
    }
}

TAllocationGroupResourcesMap TAssignmentPlanUpdateContext::GetGroupedNeededResources(
    const TOperationPtr& operation,
    const TPoolTreeOperationElement* operationElement) const
{
    // TODO(eshcherbin): In full mode just return operation's grouped needed resources.

    // NB(eshcherbin): This is a temporary hack. In dry-run mode operation's needed resources are not consistent
    // with the policy's perceived resource usage (because there are no allocations in the dry-run policy).
    // Thus, we need to approximate the known total resource demand by fake grouped needed resources.
    YT_VERIFY(operation->InitialGroupedNeededResources());

    if (operation->InitialGroupedNeededResources()->empty()) {
        return *operation->InitialGroupedNeededResources();
    }

    // For vanilla operations we pretend that they always want what they wanted in the very beginning.
    if (operation->GetType() == EOperationType::Vanilla) {
        return *operation->InitialGroupedNeededResources();
    }

    // For all other operations we pretend that all their allocations are uniform.
    const auto& [allocationGroupName, allocationGroup] = *operation->InitialGroupedNeededResources()->begin();
    auto approximateAllocationGroup = allocationGroup;
    approximateAllocationGroup.AllocationCount = 0;

    TJobResources approximateResourceDemand;
    while (!Dominates(approximateResourceDemand, operationElement->ResourceDemand()) &&
        approximateAllocationGroup.AllocationCount < allocationGroup.AllocationCount)
    {
        approximateResourceDemand += approximateAllocationGroup.MinNeededResources;
        ++approximateAllocationGroup.AllocationCount;
    }

    return TAllocationGroupResourcesMap{{allocationGroupName, approximateAllocationGroup}};
}

void TAssignmentPlanUpdateContext::IncreaseOperationUsage(const TOperationPtr& operation, const TJobResources& resourceDelta)
{
    const TPoolTreeElement* treeElement = GetOperationElement(operation);
    YT_VERIFY(treeElement);

    while (treeElement) {
        AttributesList_.AttributesOf(treeElement).AssignedResourceUsage += resourceDelta;
        treeElement = treeElement->GetParent();
    }
}

void TAssignmentPlanUpdateContext::PreemptAllOperationAssignments(
    const TOperationPtr& operation,
    EAllocationPreemptionReason preemptionReason,
    const std::string& preemptionDescription)
{
    for (const auto& assignment : GetItems(operation->Assignments())) {
        PreemptAssignment(assignment, preemptionReason, preemptionDescription);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
