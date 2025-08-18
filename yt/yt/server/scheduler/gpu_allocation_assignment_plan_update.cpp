#include "gpu_allocation_assignment_plan_update.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/yson/consumer.h>

namespace NYT::NScheduler {

using namespace NLogging;
using namespace NYTree;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

int TModuleState::GetNodeCount() const
{
    return std::ssize(AvailableNodes_);
}

int TModuleState::GetUnreservedNodeCount() const
{
    return GetNodeCount() - ReservedNodeCount_;
}

void TModuleState::AddFullHostBoundOperation(const TGpuSchedulerOperationPtr& operation)
{
    YT_VERIFY(operation->IsFullHost());

    InsertOrCrash(FullHostBoundOperations_, operation.Get());
    // TODO(eshcherbin): Support changing needed allocation count in operations.
    ReservedNodeCount_ += operation->GetInitialNeededAllocationCount();
}

void TModuleState::RemoveFullHostBoundOperation(const TGpuSchedulerOperationPtr& operation)
{
    EraseOrCrash(FullHostBoundOperations_, operation.Get());
    ReservedNodeCount_ -= operation->GetInitialNeededAllocationCount();
}

void FormatValue(TStringBuilderBase* builder, const TModuleState& state, TStringBuf /*spec*/)
{
    builder->AppendFormat("{NodeCount: %v, UnreservedNodeCount: %v, FullHostBoundOperationCount: %v}",
        state.GetNodeCount(),
        state.GetUnreservedNodeCount(),
        std::ssize(state.FullHostBoundOperations()));
}

void Serialize(const TModuleState& state, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("node_count").Value(state.GetNodeCount())
            .Item("unreserved_node_count").Value(state.GetUnreservedNodeCount())
            .Item("full_host_bound_operation_count").Value(std::ssize(state.FullHostBoundOperations()))
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

bool operator <(const TOperationModuleBindingOutcome& lhs, const TOperationModuleBindingOutcome& rhs)
{
    if (lhs.TotalEvictionPenalty != rhs.TotalEvictionPenalty) {
        return lhs.TotalEvictionPenalty < rhs.TotalEvictionPenalty;
    }

    return lhs.RemainingUnreservedNodeCount < rhs.RemainingUnreservedNodeCount;
}

void FormatValue(TStringBuilderBase* builder, const TOperationModuleBindingOutcome& outcome, TStringBuf /*spec*/)
{
    builder->AppendFormat("{RemainingUnreservedNodeCount: %v, TotalEvictionPenalty: %v, OperationToEvictCount: %v}",
        outcome.RemainingUnreservedNodeCount,
        outcome.TotalEvictionPenalty,
        std::ssize(outcome.OperationsToEvict));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TGpuAllocationAssignmentPlanUpdateExecutor::TGpuAllocationAssignmentPlanUpdateExecutor(
    const TGpuSchedulerOperationMap& operations,
    const TGpuSchedulerNodeMap& nodes,
    TInstant now,
    TGpuAllocationSchedulerConfigPtr config,
    NLogging::TLogger logger)
    : Operations_(operations)
    , Nodes_(nodes)
    , Now_(now)
    , Config_(std::move(config))
    , Logger(std::move(logger))
{ }

void TGpuAllocationAssignmentPlanUpdateExecutor::Run()
{
    YT_LOG_INFO("Starting GPU allocation assignment plan update");

    InitializeModuleStates();

    ProcessFullHostModuleBoundOperations();
    ProcessRegularOperations();
}

void TGpuAllocationAssignmentPlanUpdateExecutor::InitializeModuleStates()
{
    ModuleStates_.reserve(Config_->Modules.size());
    for (const auto& module : Config_->Modules) {
        ModuleStates_.emplace(module, NDetail::TModuleState{});
    }

    // Initialize nodes.
    std::vector<std::pair<std::string, std::optional<std::string>>> nodesWithUnknownModule;
    for (const auto& [_, node] : Nodes_) {
        if (!node->Descriptor() || !node->SchedulingModule()) {
            continue;
        }

        auto it = ModuleStates_.find(*node->SchedulingModule());
        if (it == ModuleStates_.end()) {
            nodesWithUnknownModule.emplace_back(node->Descriptor()->GetDefaultAddress(), node->SchedulingModule());
            continue;
        }

        auto& moduleState = it->second;
        moduleState.AvailableNodes().push_back(node.Get());

        SchedulableNodes_.push_back(node.Get());
    }

    // Initialize operations.
    std::vector<std::pair<TOperationId, std::string>> operationsWithUnknownModule;
    for (const auto& [operationId, operation] : Operations_) {
        if (!operation->IsFullHostModuleBound()) {
            continue;
        }

        for (const auto& assignment : operation->Assignments()) {
            YT_VERIFY(operation->IsPreemptible() == assignment->Preemptible);
        }

        if (!operation->SchedulingModule()) {
            continue;
        }

        // Sanity check.
        if (auto usedModule = operation->GetUsedSchedulingModule()) {
            YT_VERIFY(usedModule == operation->SchedulingModule());
        }

        // Preemptible operation cannot be bound to a module.
        if (operation->IsPreemptible()) {
            operation->SchedulingModule().reset();
            continue;
        }

        auto it = ModuleStates_.find(*operation->SchedulingModule());
        if (it == ModuleStates_.end()) {
            operationsWithUnknownModule.emplace_back(operationId, *operation->SchedulingModule());
            continue;
        }

        auto& moduleState = it->second;
        moduleState.AddFullHostBoundOperation(operation.Get());
    }

    // Logging.
    YT_LOG_INFO("Initialized module states (ModuleStates: %v)", ModuleStates_);

    // TODO(eshcherbin): Add alerts.
    if (!nodesWithUnknownModule.empty()) {
        int nodesWithUnknownModuleCount = std::ssize(nodesWithUnknownModule);

        static constexpr int MaxNodeWithUnknownModuleSampleSize = 10;
        nodesWithUnknownModule.resize(std::min(nodesWithUnknownModuleCount, MaxNodeWithUnknownModuleSampleSize));
        YT_LOG_INFO("Found nodes with unknown module (Count: %v, Sample: %v)",
            nodesWithUnknownModuleCount,
            nodesWithUnknownModule);
    }
    if (!operationsWithUnknownModule.empty()) {
        int operationsWithUnknownModuleCount = std::ssize(operationsWithUnknownModule);

        static constexpr int MaxOperationWithUnknownModuleSampleSize = 10;
        nodesWithUnknownModule.resize(std::min(operationsWithUnknownModuleCount, MaxOperationWithUnknownModuleSampleSize));
        YT_LOG_WARNING("Found operations with unknown module (Count: %v, Sample: %v)",
            operationsWithUnknownModuleCount,
            nodesWithUnknownModule);
    }
}

void TGpuAllocationAssignmentPlanUpdateExecutor::ProcessFullHostModuleBoundOperations()
{
    // 1. Initialize.
    std::vector<TGpuSchedulerOperationPtr> fullHostModuleBoundOperations;
    for (const auto& [_, operation] : Operations_) {
        if (operation->IsFullHostModuleBound()) {
            fullHostModuleBoundOperations.push_back(operation);
        }
    }

    YT_LOG_DEBUG("Collected full-host module-bound operations (Count: %v)", std::ssize(fullHostModuleBoundOperations));

    // 2. Process priority full-host module-bound operations.
    std::vector<TGpuSchedulerOperationPtr> priorityOperationsToPlan;
    for (const auto& operation : fullHostModuleBoundOperations) {
        if (operation->GetReadyToAssignNeededAllocationCount() > 0 && ShouldUsePriorityModuleBinding(operation)) {
            priorityOperationsToPlan.push_back(operation);
        }
    }

    PlanFullHostModuleBoundOperations(priorityOperationsToPlan, /*priorityModuleBinding*/ true);

    // 3. Process regular full-host module-bound operations.
    // NB(eshcherbin): Some operations could have been evicted, so we need to do a whole new pass over |fullHostModuleBoundOperations|.
    std::vector<TGpuSchedulerOperationPtr> regularOperationsToPlan;
    for (const auto& operation : fullHostModuleBoundOperations) {
        if (operation->GetReadyToAssignNeededAllocationCount() > 0) {
            regularOperationsToPlan.push_back(operation);
        }
    }

    PlanFullHostModuleBoundOperations(regularOperationsToPlan);
}

void TGpuAllocationAssignmentPlanUpdateExecutor::PlanFullHostModuleBoundOperations(
    std::vector<TGpuSchedulerOperationPtr>& operationsToPlan,
    bool priorityModuleBinding)
{
    if (operationsToPlan.empty()) {
        return;
    }

    YT_LOG_DEBUG("Planning full-host module-bound operations (Count: %v, PriorityModuleBinding: %v)",
        std::ssize(operationsToPlan),
        priorityModuleBinding);

    SortFullHostModuleBoundOperations(operationsToPlan);

    for (const auto& operation : operationsToPlan) {
        // Sanity check.
        YT_VERIFY(!operation->IsPreemptible());

        if (!operation->SchedulingModule() && !BindFullHostOperationToModule(operation, priorityModuleBinding)) {
            continue;
        }

        if (!operation->WaitingForAssignmentsSince()) {
            operation->WaitingForAssignmentsSince() = Now_;
        }

        YT_LOG_DEBUG(
            "Planning full-host module-bound operation "
            "(Module: %v, ReadyToAssignGroupedNeededResources: %v, "
            "WaitingForAssignmentsSince: %v, OperationId: %v)",
            operation->SchedulingModule(),
            operation->ReadyToAssignGroupedNeededResources(),
            operation->WaitingForAssignmentsSince(),
            operation->GetId());

        YT_VERIFY(operation->SchedulingModule());

        // TODO(eshcherbin): Deal with modules that can change between updates.
        // TODO(eshcherbin): Reconsider module if operation cannot be scheduled for too long.
        auto& moduleState = GetOrCrash(ModuleStates_, *operation->SchedulingModule());
        for (const auto& [allocationGroupName, allocationGroupResources] : operation->ReadyToAssignGroupedNeededResources()) {
            // First we try to schedule allocations without preemption.
            PlanAllocationGroup(
                operation,
                allocationGroupName,
                allocationGroupResources,
                &moduleState.AvailableNodes());

            // Then we try to schedule allocations using regular preemption.
            if (operation->IsStarving()) {
                PlanAllocationGroupWithPreemption(
                    operation,
                    allocationGroupName,
                    allocationGroupResources,
                    &moduleState.AvailableNodes());
            }

            // Finally, we try to schedule allocations using full-host aggressive preemption.
            if (ShouldUseFullHostAggressivePreemption(operation)) {
                PlanAllocationGroupWithPreemption(
                    operation,
                    allocationGroupName,
                    allocationGroupResources,
                    &moduleState.AvailableNodes(),
                    /*useFullHostAggressivePreemption*/ true);
            }
        }

        if (operation->GetReadyToAssignNeededAllocationCount() == 0) {
            operation->WaitingForAssignmentsSince().reset();
        }
    }
}

void TGpuAllocationAssignmentPlanUpdateExecutor::SortFullHostModuleBoundOperations(std::vector<TGpuSchedulerOperationPtr>& operations)
{
    auto comparator = [&] (const TGpuSchedulerOperationPtr& lhs, const TGpuSchedulerOperationPtr& rhs) {
        // This happens in case some of the operation's assignments are removed,
        // and we want to reschedule the allocations ASAP.
        if (lhs->SchedulingModule().has_value() != rhs->SchedulingModule().has_value()) {
            return lhs->SchedulingModule().has_value();
        }

        // The narrower operation's module selection is, the sooner we want to process it.
        // This mechanism could be abused right now, but no real problems have been observed as yet.
        auto lhsSpecifiedModuleCount = lhs->SpecifiedSchedulingModules()
            ? lhs->SpecifiedSchedulingModules()->size()
            : ModuleStates_.size();
        auto rhsSpecifiedModuleCount = rhs->SpecifiedSchedulingModules()
            ? rhs->SpecifiedSchedulingModules()->size()
            : ModuleStates_.size();
        if (lhsSpecifiedModuleCount != rhsSpecifiedModuleCount) {
            return lhsSpecifiedModuleCount < rhsSpecifiedModuleCount;
        }

        // Finally, the bigger operation is, the sooner we want to process it.
        return lhs->GetInitialNeededAllocationCount() > rhs->GetInitialNeededAllocationCount();
    };
    std::ranges::sort(operations, comparator);
}

bool TGpuAllocationAssignmentPlanUpdateExecutor::ShouldUseFullHostAggressivePreemption(const TGpuSchedulerOperationPtr& operation) const
{
    return operation->IsFullHostModuleBound() &&
        operation->WaitingForAssignmentsSince() &&
        *operation->WaitingForAssignmentsSince() + Config_->FullHostAggressivePreemptionTimeout < Now_;
}

bool TGpuAllocationAssignmentPlanUpdateExecutor::ShouldUsePriorityModuleBinding(const TGpuSchedulerOperationPtr& operation) const
{
    return operation->IsPriorityModuleBindingEnabled() &&
        operation->WaitingForModuleBindingSince() &&
        *operation->WaitingForModuleBindingSince() + Config_->PriorityModuleBindingTimeout < Now_;
}

bool TGpuAllocationAssignmentPlanUpdateExecutor::BindFullHostOperationToModule(
    const TGpuSchedulerOperationPtr& operation,
    bool priorityModuleBinding)
{
    const int allocationCount = operation->GetInitialNeededAllocationCount();
    std::vector<std::string> feasibleModules;
    for (const auto& [module, moduleState] : ModuleStates_) {
        if (const auto& specifiedModules = operation->SpecifiedSchedulingModules();
            specifiedModules && !specifiedModules->contains(module))
        {
            continue;
        }

        if (moduleState.GetNodeCount() >= allocationCount) {
            feasibleModules.push_back(module);
        }
    }

    auto operationUsedModule = operation->GetUsedSchedulingModule();
    YT_LOG_DEBUG(
        "Trying to bind a full-host operation to a module "
        "(AllModules: %v, SpecifiedModules: %v, FeasibleModules: %v, OperationUsedModule: %v, AllocationCount: %v, "
        "WaitingForModuleBindingSince: %v, PriorityModuleBinding: %v, PriorityModuleBindingDeadline: %v, OperationId: %v)",
        GetKeys(ModuleStates_),
        operation->SpecifiedSchedulingModules(),
        feasibleModules,
        operationUsedModule,
        allocationCount,
        operation->WaitingForModuleBindingSince(),
        priorityModuleBinding,
        priorityModuleBinding
            ? std::optional{Now_ + Config_->PriorityModuleBindingTimeout}
            : std::nullopt,
        operation->GetId());

    std::vector<std::pair<NDetail::TOperationModuleBindingOutcome, std::string>> possibleModuleBindings;
    for (const auto& module : feasibleModules) {
        if (auto outcome = ConsiderModuleForFullHostOperation(operation, module, priorityModuleBinding)) {
            YT_LOG_DEBUG("Possible module binding outcome (Module: %v, Outcome: %v, OperationId: %v)",
                module,
                *outcome,
                operation->GetId());

            possibleModuleBindings.emplace_back(std::move(*outcome), module);
        }
    }

    if (possibleModuleBindings.empty()) {
        YT_LOG_DEBUG("Failed to choose a suitable module for operation (OperationId: %v)", operation->GetId());

        if (!operation->WaitingForModuleBindingSince()) {
            operation->WaitingForModuleBindingSince() = Now_;
        }

        return false;
    }

    const auto& [bestModuleBindingOutcome, bestModule] = *std::ranges::min_element(possibleModuleBindings);

    YT_LOG_DEBUG("Binding full-host operation to module (Module: %v, OperationId: %v)",
        bestModule,
        operation->GetId());

    if (operationUsedModule && (*operationUsedModule != bestModule)) {
        YT_LOG_DEBUG("Preempting all operation's assignments in other module (OperationUsedModule: %v, OperationId: %v)",
            operationUsedModule,
            operation->GetId());

        // NB(eshcherbin): This operation will not have the full ready to assign allocation count on this iteration,
        // so it will not be fully assigned. However, on the next iteration everything will be alright.
        PreemptAllOperationAssignments(
            operation,
            EAllocationPreemptionReason::OperationBoundToOtherModule,
            Format("Preempted because operation was bound to other scheduling module %v", bestModule));
    }

    auto& moduleState = GetOrCrash(ModuleStates_, bestModule);
    for (const auto& evictedOperation : bestModuleBindingOutcome.OperationsToEvict) {
        YT_LOG_DEBUG("Evicting operation from module in favour of a priority operation (Module: %v, OperationId: %v, PriorityOperationId: %v)",
            bestModule,
            evictedOperation->GetId(),
            operation->GetId());

        YT_VERIFY(bestModule == evictedOperation->SchedulingModule());

        moduleState.RemoveFullHostBoundOperation(evictedOperation);

        evictedOperation->SchedulingModule().reset();

        PreemptAllOperationAssignments(
            evictedOperation,
            EAllocationPreemptionReason::EvictionFromSchedulingModule,
            Format("Preempted due to operation's eviction from scheduling module in favour of priority operation %v", operation->GetId()));
    }

    operation->WaitingForModuleBindingSince().reset();
    operation->SchedulingModule() = bestModule;

    moduleState.AddFullHostBoundOperation(operation.Get());

    return true;
}

std::optional<NDetail::TOperationModuleBindingOutcome> TGpuAllocationAssignmentPlanUpdateExecutor::ConsiderModuleForFullHostOperation(
    const TGpuSchedulerOperationPtr& operation,
    const std::string& module,
    bool priorityModuleBinding) const
{
    const auto& moduleState = GetOrCrash(ModuleStates_, module);
    const int allocationCount = operation->GetInitialNeededAllocationCount();

    YT_LOG_DEBUG(
        "Considering module for full-host operation (Module: %v, ModuleState: %v, AllocationCount: %v, OperationId: %v)",
        module,
        moduleState,
        allocationCount,
        operation->GetId());

    // NB(eshcherbin): If operation already has assignments in some other module, we will need to preempt them.
    // Thus, we increase eviction penalty to choose this module if possible.
    const auto operationUsedModule = operation->GetUsedSchedulingModule();
    const int availableNodeCount = moduleState.GetUnreservedNodeCount();
    if (availableNodeCount >= allocationCount) {
        return NDetail::TOperationModuleBindingOutcome{
            .RemainingUnreservedNodeCount = availableNodeCount - allocationCount,
            .TotalEvictionPenalty = (operationUsedModule && (*operationUsedModule != module))
                ? static_cast<int>(std::ssize(operation->Assignments()))
                : 0,
        };
    }

    if (priorityModuleBinding) {
        std::vector<TGpuSchedulerOperation*> availableForEvictionOperations;
        for (auto* operation : moduleState.FullHostBoundOperations()) {
            if (!operation->IsPriorityModuleBindingEnabled()) {
                availableForEvictionOperations.push_back(operation);
            }
        }

        int freedNodeCount;
        std::vector<TGpuSchedulerOperation*> operationsToEvict;
        bool success = FindOperationsToEvict(
            availableForEvictionOperations,
            /*neededNodeCount*/ allocationCount - availableNodeCount,
            &operationsToEvict,
            &freedNodeCount);
        if (success) {
            return NDetail::TOperationModuleBindingOutcome{
                .RemainingUnreservedNodeCount = availableNodeCount + freedNodeCount - allocationCount,
                .TotalEvictionPenalty = freedNodeCount,
                .OperationsToEvict = std::move(operationsToEvict),
            };
        }
    }

    return {};
}

//! This greedy algorithm finds a subset of |availableOperations| to evict,
//! such that the total freed node count exceeds |neededNodeCount|.
//! If total node count reserved for all available operations is not enough, returns false.
bool TGpuAllocationAssignmentPlanUpdateExecutor::FindOperationsToEvict(
    const std::vector<TGpuSchedulerOperation*>& availableOperations,
    int neededNodeCount,
    std::vector<TGpuSchedulerOperation*>* operationsToEvict,
    int* freedNodeCount) const
{
    auto getReservedNodeCount = [] (const TGpuSchedulerOperation* operation) {
        return operation->GetInitialNeededAllocationCount();
    };

    auto willSatisfyNeededNodeCountAfterOperation = [&] (const TGpuSchedulerOperation* operation) {
        return *freedNodeCount + getReservedNodeCount(operation) >= neededNodeCount;
    };

    *operationsToEvict = availableOperations;
    std::ranges::sort(
        *operationsToEvict,
        /*comp*/ std::greater{},
        /*proj*/ getReservedNodeCount);

    *freedNodeCount = 0;
    auto currentOperationIt = begin(*operationsToEvict);
    while (currentOperationIt != operationsToEvict->end() &&
        !willSatisfyNeededNodeCountAfterOperation(*currentOperationIt))
    {
        *freedNodeCount += getReservedNodeCount(*currentOperationIt);
        ++currentOperationIt;
    }

    if (currentOperationIt == operationsToEvict->end()) {
        return false;
    }

    auto lastAddedOperationIt = currentOperationIt;
    while (currentOperationIt != operationsToEvict->end() &&
        willSatisfyNeededNodeCountAfterOperation(*currentOperationIt))
    {
        *lastAddedOperationIt = std::move(*currentOperationIt);
        ++currentOperationIt;
    }

    *freedNodeCount += getReservedNodeCount(*lastAddedOperationIt);

    operationsToEvict->erase(std::ranges::next(lastAddedOperationIt), end(*operationsToEvict));

    return true;
}

void TGpuAllocationAssignmentPlanUpdateExecutor::ProcessRegularOperations()
{
    // 1. Initialize.
    std::vector<TGpuSchedulerOperationPtr> operationsToPlan;
    for (const auto& [_, operation] : Operations_ ) {
        if (operation->IsFullHostModuleBound()) {
            continue;
        }

        if (operation->GetReadyToAssignNeededAllocationCount() > 0) {
            operationsToPlan.push_back(operation);
        }
    }

    if (operationsToPlan.empty()) {
        return;
    }

    YT_LOG_DEBUG("Planning non gang operations (Count: %v)",
        std::ssize(operationsToPlan));

    // 2. Sort operations.
    std::ranges::sort(
        operationsToPlan,
        [&] (const TGpuSchedulerOperationPtr& lhs, const TGpuSchedulerOperationPtr& rhs) {
            // Usually, vanilla operations are used for model training and map operations are used for batch inference.
            bool lhsVanilla = lhs->GetType() == EOperationType::Vanilla;
            bool rhsVanilla = rhs->GetType() == EOperationType::Vanilla;
            if (lhsVanilla != rhsVanilla) {
                return lhsVanilla;
            }

            // Operations with bigger allocations are processed first.
            const auto& lhsAllocationResources = lhs->ReadyToAssignGroupedNeededResources().begin()->second.MinNeededResources;
            const auto& rhsAllocationResources = rhs->ReadyToAssignGroupedNeededResources().begin()->second.MinNeededResources;
            if (lhsAllocationResources.GetGpu() != rhsAllocationResources.GetGpu()) {
                return lhsAllocationResources.GetGpu() > rhsAllocationResources.GetGpu();
            }

            // Finally, the bigger operation is, the sooner we want to process it.
            return lhs->GetReadyToAssignNeededAllocationCount() > rhs->GetReadyToAssignNeededAllocationCount();
        });

    // 3. Plan assignments.
    for (const auto& operation : operationsToPlan) {
        for (const auto& [allocationGroupName, allocationGroupResources] : operation->ReadyToAssignGroupedNeededResources()) {
            PlanAllocationGroup(
                operation,
                allocationGroupName,
                allocationGroupResources,
                &SchedulableNodes_);

            if (operation->IsStarving()) {
                PlanAllocationGroupWithPreemption(
                    operation,
                    allocationGroupName,
                    allocationGroupResources,
                    &SchedulableNodes_);
            }
        }
    }
}

void TGpuAllocationAssignmentPlanUpdateExecutor::AddAssignment(const TGpuSchedulerAssignmentPtr& assignment)
{
    YT_LOG_DEBUG("Adding assignment (AllocationGroupName: %v, ResourceUsage: %v, NodeAddress: %v, OperationId: %v)",
        assignment->AllocationGroupName,
        assignment->ResourceUsage,
        assignment->Node->Descriptor()->GetDefaultAddress(),
        assignment->Operation->GetId());

    assignment->Node->AddAssignment(assignment);
    assignment->Operation->AddAssignment(assignment);
}

void TGpuAllocationAssignmentPlanUpdateExecutor::PreemptAssignment(
    const TGpuSchedulerAssignmentPtr& assignment,
    EAllocationPreemptionReason preemptionReason,
    std::string preemptionDescription)
{
    YT_LOG_DEBUG(
        "Preempting assignment (Reason: %v, Description: %v, "
        "AllocationGroupName: %v, ResourceUsage: %v, NodeAddress: %v, OperationId: %v)",
        preemptionReason,
        preemptionDescription,
        assignment->AllocationGroupName,
        assignment->ResourceUsage,
        assignment->Node->Descriptor()->GetDefaultAddress(),
        assignment->Operation->GetId());

    assignment->PreemptionReason = preemptionReason;
    assignment->PreemptionDescription = std::move(preemptionDescription);
    assignment->Node->PreemptAssignment(assignment);
    assignment->Operation->RemoveAssignment(assignment);
}

void TGpuAllocationAssignmentPlanUpdateExecutor::PreemptAllOperationAssignments(
    const TGpuSchedulerOperationPtr& operation,
    EAllocationPreemptionReason preemptionReason,
    const std::string& preemptionDescription)
{
    // NB(eshcherbin): Copy assignments with |GetItems|, because the set will be modified.
    for (const auto& assignment : GetItems(operation->Assignments())) {
        PreemptAssignment(assignment, preemptionReason, preemptionDescription);
    }
}

// TODO(eshcherbin): Support non-empty assignments.
NDetail::TPreemptionPenalty TGpuAllocationAssignmentPlanUpdateExecutor::GetAssignmentPreemptionPenalty(
    const TGpuSchedulerAssignmentPtr& assignment) const
{
    return static_cast<NDetail::TPreemptionPenalty>(Config_->MinAssignmentPreemptibleDuration.Seconds()) *
        assignment->ResourceUsage.GetGpu();
}

void TGpuAllocationAssignmentPlanUpdateExecutor::PlanAllocationGroup(
    const TGpuSchedulerOperationPtr& operation,
    const std::string& allocationGroupName,
    const TAllocationGroupResources allocationGroupResources,
    std::vector<TGpuSchedulerNode*>* availableNodes)
{
    if (allocationGroupResources.AllocationCount == 0) {
        return;
    }

    YT_LOG_DEBUG("Planning allocation group for operation (AllocationGroup: {Name: %v, Resources: %v}, OperationId: %v)",
        allocationGroupName,
        allocationGroupResources,
        operation->GetId());

    TAllocationGroupPlanner planner(
        operation,
        allocationGroupName,
        allocationGroupResources,
        availableNodes,
        this);
    planner.Run();

    YT_LOG_DEBUG("Finished planning allocation group for operation (PlannedAssignmentCount: %v, AllocationGroup: {Name: %v, Resources: %v}, OperationId: %v)",
        planner.GetPlannedAssignmentCount(),
        allocationGroupName,
        allocationGroupResources,
        operation->GetId());
}

void TGpuAllocationAssignmentPlanUpdateExecutor::PlanAllocationGroupWithPreemption(
    const TGpuSchedulerOperationPtr& operation,
    const std::string& allocationGroupName,
    TAllocationGroupResources allocationGroupResources,
    std::vector<TGpuSchedulerNode*>* availableNodes,
    bool useFullHostAggressivePreemption)
{
    if (allocationGroupResources.AllocationCount == 0) {
        return;
    }

    YT_LOG_DEBUG(
        "Planning allocation group for operation with preemption "
        "(AllocationGroup: {Name: %v, Resources: %v}, UseFullHostAggressivePreemption: %v, OperationId: %v)",
        allocationGroupName,
        allocationGroupResources,
        useFullHostAggressivePreemption,
        operation->GetId());

    TPreemptiveAllocationGroupPlanner planner(
        operation,
        allocationGroupName,
        allocationGroupResources,
        availableNodes,
        useFullHostAggressivePreemption,
        this);
    planner.Run();

    YT_LOG_DEBUG(
        "Finished planning allocation group for operation with preemption "
        "(PlannedAssignmentCount: %v, PreemptedAssignmentCount: %v, "
        "AllocationGroup: {Name: %v, Resources: %v}, OperationId: %v)",
        planner.GetPlannedAssignmentCount(),
        planner.GetPreemptedAssignmentCount(),
        allocationGroupName,
        allocationGroupResources,
        operation->GetId());
}

////////////////////////////////////////////////////////////////////////////////

TGpuAllocationAssignmentPlanUpdateExecutor::TAllocationGroupPlannerBase::TAllocationGroupPlannerBase(
    const TGpuSchedulerOperationPtr& operation,
    const std::string& allocationGroupName,
    const TAllocationGroupResources& allocationGroupResources,
    TGpuAllocationAssignmentPlanUpdateExecutor* host)
    : Operation_(operation)
    , AllocationGroupName_(allocationGroupName)
    , AllocationGroupResources_(allocationGroupResources)
    , Host_(host)
{ }

void TGpuAllocationAssignmentPlanUpdateExecutor::TAllocationGroupPlannerBase::Run()
{
    while (PlannedAssignmentCount_ < AllocationGroupResources_.AllocationCount) {
        auto* node = GetBestAvailableNode();
        if (!node) {
            break;
        }

        AddAssignmentToNode(node);
        ++PlannedAssignmentCount_;
    }
}

bool TGpuAllocationAssignmentPlanUpdateExecutor::TAllocationGroupPlannerBase::CanAddAssignmentToNode(
    TGpuSchedulerNode* node,
    const TJobResources& discount) const
{
    return Dominates(
        node->Descriptor()->ResourceLimits,
        (node->AssignedResourceUsage() - discount) + AllocationGroupResources_.MinNeededResources.ToJobResources());
}

void TGpuAllocationAssignmentPlanUpdateExecutor::TAllocationGroupPlannerBase::AddAssignmentToNode(
    TGpuSchedulerNode* node)
{
    auto assignment = New<TGpuSchedulerAssignment>(
        AllocationGroupName_,
        AllocationGroupResources_.MinNeededResources,
        Operation_.Get(),
        node);
    Host_->AddAssignment(assignment);
}

////////////////////////////////////////////////////////////////////////////////

TGpuAllocationAssignmentPlanUpdateExecutor::TAllocationGroupPlanner::TAllocationGroupPlanner(
    const TGpuSchedulerOperationPtr& operation,
    const std::string& allocationGroupName,
    const TAllocationGroupResources& allocationGroupResources,
    std::vector<TGpuSchedulerNode*>* availableNodes,
    TGpuAllocationAssignmentPlanUpdateExecutor* host)
    : TAllocationGroupPlannerBase(operation, allocationGroupName, allocationGroupResources, host)
    , AvailableNodes_(availableNodes)
{
    std::ranges::sort(
        *AvailableNodes_,
        [&] (const auto* lhs, const auto* rhs) {
            return lhs->GetUnassignedGpuCount() < rhs->GetUnassignedGpuCount();
        });
    NextNodeIt_ = AvailableNodes_->begin();
}

TGpuSchedulerNode* TGpuAllocationAssignmentPlanUpdateExecutor::TAllocationGroupPlanner::GetBestAvailableNode()
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

TGpuAllocationAssignmentPlanUpdateExecutor::TPreemptiveAllocationGroupPlanner::TPreemptiveAllocationGroupPlanner(
    const TGpuSchedulerOperationPtr& operation,
    const std::string& allocationGroupName,
    const TAllocationGroupResources& allocationGroupResources,
    std::vector<TGpuSchedulerNode*>* availableNodes,
    bool useFullHostAggressivePreemption,
    TGpuAllocationAssignmentPlanUpdateExecutor* host)
    : TAllocationGroupPlannerBase(operation, allocationGroupName, allocationGroupResources, host)
    , UseFullHostAggressivePreemption_(useFullHostAggressivePreemption)
    , PreemptionReason_(UseFullHostAggressivePreemption_
        ? EAllocationPreemptionReason::FullHostAggressivePreemption
        : EAllocationPreemptionReason::Preemption)
    , PreemptionDescription_(useFullHostAggressivePreemption
        ? Format("Aggressively preempted to plan an assignment for full-host operation %v", operation->GetId())
        : Format("Preempted to plan an assignment for operation %v", operation->GetId()))
{
    NodeStates_.reserve(availableNodes->size());
    NodeHeap_.reserve(availableNodes->size());
    for (auto* node : *availableNodes) {
        auto& nodeState = NodeStates_[node];
        for (const auto& assignment : node->Assignments()) {
            bool preemptible = assignment->Preemptible ||
                (UseFullHostAggressivePreemption_ && !assignment->Operation->IsFullHostModuleBound());
            if (preemptible) {
                nodeState.PreemptibleAssignments.push_back(assignment);
                nodeState.PreemptibleResourceUsage += assignment->ResourceUsage;
            }
        }

        std::ranges::sort(
            nodeState.PreemptibleAssignments,
            /*comp*/ std::greater{},
            /*proj*/ [&] (const auto& assignment) { return Host_->GetAssignmentPreemptionPenalty(assignment); });

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
NDetail::TPreemptionPenalty TGpuAllocationAssignmentPlanUpdateExecutor::TPreemptiveAllocationGroupPlanner::GetNextPreemptionPenaltyForNode(TGpuSchedulerNode* node) const
{
    const auto& nodeState = GetOrCrash(NodeStates_, node);
    NDetail::TPreemptionPenalty penalty = 0;
    TJobResources preliminaryPreemptedResources;
    auto it = nodeState.PreemptibleAssignments.rbegin();
    while (!CanAddAssignmentToNode(node, /*discount*/ preliminaryPreemptedResources)) {
        YT_VERIFY(it != nodeState.PreemptibleAssignments.rend());

        const auto& assignment = *it;
        preliminaryPreemptedResources += assignment->ResourceUsage;
        penalty += Host_->GetAssignmentPreemptionPenalty(assignment);
    }

    return penalty;
}

void TGpuAllocationAssignmentPlanUpdateExecutor::TPreemptiveAllocationGroupPlanner::AddAssignmentToNode(TGpuSchedulerNode* node)
{
    auto& nodeState = GetOrCrash(NodeStates_, node);
    while (!CanAddAssignmentToNode(node)) {
        YT_VERIFY(!nodeState.PreemptibleAssignments.empty());

        auto preemptibleAssignment = nodeState.PreemptibleAssignments.back();
        nodeState.PreemptibleAssignments.pop_back();
        nodeState.PreemptibleResourceUsage -= preemptibleAssignment->ResourceUsage;

        Host_->PreemptAssignment(preemptibleAssignment, PreemptionReason_, PreemptionDescription_);

        ++PreemptedAssignmentCount_;
    }

    TAllocationGroupPlannerBase::AddAssignmentToNode(node);

    if (CanAddAssignmentToNode(node, nodeState.PreemptibleResourceUsage)) {
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

TGpuSchedulerNode* TGpuAllocationAssignmentPlanUpdateExecutor::TPreemptiveAllocationGroupPlanner::GetBestAvailableNode()
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
