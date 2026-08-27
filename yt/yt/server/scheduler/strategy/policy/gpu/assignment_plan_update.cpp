#include "assignment_plan_update.h"

#include "allocation_group_planner.h"
#include "private.h"
#include "helpers.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>
#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/yson/consumer.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

using namespace NLogging;
using namespace NConcurrency;
using namespace NYTree;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TModuleReservation::TModuleReservation(const TOperationPtr& operation)
    : Item_(operation)
    // Here we assume that operation's allocation count stays the same the whole time.
    // TODO(eshcherbin): (!) Change this assumption to something more realistic.
    , NodeCount_(operation->GetInitialNeededAllocationCount())
{
    YT_VERIFY(operation->IsFullHostModuleBound());
}

TModuleReservation::TModuleReservation(const TAssignmentPtr& assignment)
    : Item_(assignment)
    , NodeCount_(1)
{
    YT_VERIFY(assignment->Operation->IsFullHostNonGang());
}

bool TModuleReservation::IsPriorityModuleBoundOperation() const
{
    auto* operation = std::get_if<TOperationPtr>(&Item_);
    return operation && (*operation)->PriorityModuleBindingEnabled().value_or(false);
}

void FormatValue(TStringBuilderBase* builder, const TModuleReservation& reservation, TStringBuf /*spec*/)
{
    Visit(reservation.GetItem(),
        [&] (const TOperationPtr& operation) {
            builder->AppendFormat("{OperationId: %v}", operation->GetId());
        },
        [&] (const TAssignmentPtr& assignment) {
            // TODO(severovv): YT-28835 add assignment ids
            builder->AppendFormat("{AssignmentOperationId: %v}", assignment->OperationId);
        });
}

////////////////////////////////////////////////////////////////////////////////

int TModuleState::GetNodeCount() const
{
    return std::ssize(AvailableNodes_);
}

int TModuleState::GetUnreservedNodeCount() const
{
    return GetNodeCount() - ReservedNodeCount_;
}

void TModuleState::AddFullHostBoundOperation(const TOperationPtr& operation)
{
    auto reservation = New<TModuleReservation>(operation);

    EmplaceOrCrash(FullHostBoundOperationReservations_, operation.Get(), reservation);
    AddReservation(std::move(reservation));
}

void TModuleState::AddAssignment(const TAssignmentPtr& assignment)
{
    auto reservation = New<TModuleReservation>(assignment);

    FullHostNonGangAssignmentCount_ += reservation->GetNodeCount();
    AddReservation(std::move(reservation));
}

void TModuleState::RemoveReservation(const TModuleReservationPtr& reservation)
{
    Visit(reservation->GetItem(),
        [&] (const TOperationPtr& operation) {
            EraseOrCrash(FullHostBoundOperationReservations_, operation.Get());
        },
        [&] (const TAssignmentPtr& /*assignment*/) {
            FullHostNonGangAssignmentCount_ -= reservation->GetNodeCount();
        });

    ReservedNodeCount_ -= reservation->GetNodeCount();
    EraseOrCrash(ModuleReservations_, reservation);
}

void FormatValue(TStringBuilderBase* builder, const TModuleState& state, TStringBuf /*spec*/)
{
    builder->AppendFormat("{NodeCount: %v, UnreservedNodeCount: %v, FullHostBoundOperationCount: %v, FullHostNonGangAssignmentCount: %v}",
        state.GetNodeCount(),
        state.GetUnreservedNodeCount(),
        std::ssize(state.FullHostBoundOperationReservations()),
        state.GetFullHostNonGangAssignmentCount());
}

void Serialize(const TModuleState& state, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("node_count").Value(state.GetNodeCount())
            .Item("unreserved_node_count").Value(state.GetUnreservedNodeCount())
            .Item("full_host_bound_operation_count").Value(std::ssize(state.FullHostBoundOperationReservations()))
            .Item("full_host_non_gang_assignment_count").Value(state.GetFullHostNonGangAssignmentCount())
        .EndMap();
}

void TModuleState::AddReservation(TModuleReservationPtr reservation)
{
    ReservedNodeCount_ += reservation->GetNodeCount();
    EmplaceOrCrash(ModuleReservations_, std::move(reservation));
}

////////////////////////////////////////////////////////////////////////////////

bool operator<(const TOperationModuleBindingOutcome& lhs, const TOperationModuleBindingOutcome& rhs)
{
    if (lhs.TotalEvictionPenalty != rhs.TotalEvictionPenalty) {
        return lhs.TotalEvictionPenalty < rhs.TotalEvictionPenalty;
    }

    return lhs.RemainingUnreservedNodeCount < rhs.RemainingUnreservedNodeCount;
}

void FormatValue(TStringBuilderBase* builder, const TOperationModuleBindingOutcome& outcome, TStringBuf /*spec*/)
{
    builder->AppendFormat("{RemainingUnreservedNodeCount: %v, TotalEvictionPenalty: %v, ReservationsToEvictCount: %v}",
        outcome.RemainingUnreservedNodeCount,
        outcome.TotalEvictionPenalty,
        std::ssize(outcome.ReservationsToEvict));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TGpuAllocationAssignmentPlanUpdateExecutor::TGpuAllocationAssignmentPlanUpdateExecutor(
    IAssignmentPlanUpdateContext* context,
    TInstant now,
    TGpuSchedulingPolicyConfigWrapper config,
    NLogging::TLogger logger)
    : Context_(context)
    , Operations_(Context_->Operations())
    , Nodes_(Context_->Nodes())
    , Now_(now)
    , Config_(std::move(config))
    , Logger(std::move(logger))
{ }

void TGpuAllocationAssignmentPlanUpdateExecutor::Run()
{
    YT_TLOG_INFO("Starting GPU allocation assignment plan update");

    TForbidContextSwitchGuard contextSwitchGuard;

    InitializeModuleStates();

    // TODO(eshcherbin): (!) Process nodes with resource overcommit and preempt extra assignments.
    {
        NProfiling::TWallTimer fullHostModuleBoundTimer;
        ProcessFullHostModuleBoundOperations();
        Context_->GetStatistics()->FullHostModuleBoundPlanningDuration = fullHostModuleBoundTimer.GetElapsedTime();
    }
    {
        NProfiling::TWallTimer fullHostNonGangTimer;
        ProcessFullHostNonGangOperations();
        Context_->GetStatistics()->FullHostNonGangPlanningDuration = fullHostNonGangTimer.GetElapsedTime();
    }
    {
        NProfiling::TWallTimer regularTimer;
        ProcessRegularOperations();
        Context_->GetStatistics()->RegularPlanningDuration = regularTimer.GetElapsedTime();
    }
    {
        NProfiling::TWallTimer extraTimer;
        ProcessRegularOperationsWithExtraResources();
        Context_->GetStatistics()->ExtraPlanningDuration = extraTimer.GetElapsedTime();
    }

    DumpModuleStatistics();
}

void TGpuAllocationAssignmentPlanUpdateExecutor::InitializeModuleStates()
{
    const auto& modules = Config_.GetModules();
    ModuleStates_.reserve(modules.size());
    for (const auto& module : modules) {
        ModuleStates_.emplace(module, NDetail::TModuleState{});
    }

    // Initialize nodes.
    // TODO(eshcherbin): Add validation that nodes are consistent with previous assignments.
    std::vector<std::pair<std::string, std::optional<std::string>>> nodesWithUnknownModule;
    for (const auto& [_, node] : Nodes_) {
        if (!node->IsSchedulable()) {
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
    std::vector<std::pair<TAssignmentPtr, std::string>> assignmentsWithUnknownModule;
    std::vector<std::pair<TAssignmentPtr, std::string>> assignmentsOnNodeWithoutModule;
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
            operation->ResetSchedulingModule();
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

    for (const auto& [operationId, operation] : Operations_) {
        if (!operation->IsFullHostNonGang()) {
            continue;
        }

        for (const auto& assignment : operation->Assignments()) {
            if (assignment->Preemptible) {
                continue;
            }

            // A revived assignment may sit on a node whose module is not known yet.
            if (!assignment->Node->IsSchedulable()) {
                assignmentsOnNodeWithoutModule.emplace_back(assignment, assignment->Node->Address());
                continue;
            }

            auto it = ModuleStates_.find(*assignment->Node->SchedulingModule());
            if (it == ModuleStates_.end()) {
                assignmentsWithUnknownModule.emplace_back(assignment, *assignment->Node->SchedulingModule());
                continue;
            }

            auto& moduleState = it->second;
            moduleState.AddAssignment(assignment);
        }
    }

    // Logging.
    YT_TLOG_INFO("Initialized module states")
        .With("ModuleStates", ModuleStates_);

    // TODO(eshcherbin): (!) Add alerts.
    if (!nodesWithUnknownModule.empty()) {
        int nodesWithUnknownModuleCount = std::ssize(nodesWithUnknownModule);

        static constexpr int MaxNodeWithUnknownModuleSampleSize = 10;
        nodesWithUnknownModule.resize(std::min(nodesWithUnknownModuleCount, MaxNodeWithUnknownModuleSampleSize));
        YT_TLOG_INFO("Found nodes with unknown module")
            .With("Count", nodesWithUnknownModuleCount)
            .With("Sample", nodesWithUnknownModule);
    }
    if (!operationsWithUnknownModule.empty()) {
        int operationsWithUnknownModuleCount = std::ssize(operationsWithUnknownModule);

        static constexpr int MaxOperationWithUnknownModuleSampleSize = 10;
        nodesWithUnknownModule.resize(std::min(operationsWithUnknownModuleCount, MaxOperationWithUnknownModuleSampleSize));
        YT_TLOG_WARNING("Found operations with unknown module")
            .With("Count", operationsWithUnknownModuleCount)
            .With("Sample", nodesWithUnknownModule);
    }
    // TODO(severovv): add sample when assignment ids are added
    if (!assignmentsWithUnknownModule.empty()) {
        int assignmentsWithUnknownModuleCount = std::ssize(assignmentsWithUnknownModule);
        YT_TLOG_WARNING("Found assignments with unknown module")
            .With("Count", assignmentsWithUnknownModuleCount);
    }
    if (!assignmentsOnNodeWithoutModule.empty()) {
        int assignmentsOnNodeWithoutModuleCount = std::ssize(assignmentsOnNodeWithoutModule);

        static constexpr int MaxAssignmentOnNodeWithoutModuleSampleSize = 10;
        assignmentsOnNodeWithoutModule.resize(std::min(assignmentsOnNodeWithoutModuleCount, MaxAssignmentOnNodeWithoutModuleSampleSize));
        YT_TLOG_WARNING("Found assignments on nodes without module")
            .With("Count", assignmentsOnNodeWithoutModuleCount)
            .With("Sample", MakeFormattableView(
                assignmentsOnNodeWithoutModule,
                [] (TStringBuilderBase* builder, const auto& assignmentWithNodeAddress) {
                    const auto& [assignment, nodeAddress] = assignmentWithNodeAddress;
                    builder->AppendFormat("{OperationId: %v, NodeAddress: %v}", assignment->OperationId, nodeAddress);
                }));
    }
}

void TGpuAllocationAssignmentPlanUpdateExecutor::ProcessFullHostModuleBoundOperations()
{
    // 1. Initialize.
    std::vector<TOperationPtr> fullHostModuleBoundOperations;
    for (const auto& [_, operation] : Operations_) {
        if (operation->IsFullHostModuleBound()) {
            fullHostModuleBoundOperations.push_back(operation);
        }
    }

    YT_TLOG_DEBUG("Collected full-host module-bound operations")
        .With("Count", std::ssize(fullHostModuleBoundOperations));

    // 2. Process priority full-host module-bound operations.
    std::vector<TOperationPtr> priorityOperationsToPlan;
    for (const auto& operation : fullHostModuleBoundOperations) {
        if (!ShouldUsePriorityModuleBinding(operation)) {
            continue;
        }
        // NB(yaishenka): We may lose module due to the Preemptible flag setting, so we need to restore it.
        bool hasReadyToAssignAllocations = operation->GetReadyToAssignNeededAllocationCount() > 0;
        bool shouldPlanModule = !operation->IsPreemptible() && !operation->SchedulingModule() && !operation->IsZeroAssignedUsage();
        if (hasReadyToAssignAllocations || shouldPlanModule) {
            priorityOperationsToPlan.push_back(operation);
        }
    }

    PlanFullHostModuleBoundOperations(priorityOperationsToPlan, /*priorityModuleBinding*/ true);

    // 3. Process regular full-host module-bound operations.
    // NB(eshcherbin): Some operations could have been evicted, so we need to do a whole new pass over |fullHostModuleBoundOperations|.
    std::vector<TOperationPtr> regularOperationsToPlan;
    for (const auto& operation : fullHostModuleBoundOperations) {
        // NB(yaishenka): We may lose module due to the Preemptible flag setting, so we need to restore it.
        bool hasReadyToAssignAllocations = operation->GetReadyToAssignNeededAllocationCount() > 0;
        bool shouldPlanModule = !operation->IsPreemptible() && !operation->SchedulingModule() && !operation->IsZeroAssignedUsage();
        if (hasReadyToAssignAllocations || shouldPlanModule) {
            regularOperationsToPlan.push_back(operation);
        }
    }

    PlanFullHostModuleBoundOperations(regularOperationsToPlan);
}

void TGpuAllocationAssignmentPlanUpdateExecutor::PlanFullHostModuleBoundOperations(
    std::vector<TOperationPtr>& operationsToPlan,
    bool priorityModuleBinding)
{
    if (operationsToPlan.empty()) {
        return;
    }

    YT_TLOG_DEBUG("Planning full-host module-bound operations")
        .With("Count", std::ssize(operationsToPlan))
        .With("PriorityModuleBinding", priorityModuleBinding);

    SortFullHostModuleBoundOperations(operationsToPlan);

    for (const auto& operation : operationsToPlan) {
        // Sanity check.
        YT_VERIFY(!operation->IsPreemptible());

        if (ShouldResetModule(operation)) {
            EvictOperationFromSchedulingModule(operation, "Preempted after module reset");
        }

        if (!operation->SchedulingModule() && !BindFullHostOperationToModule(operation, priorityModuleBinding)) {
            continue;
        }

        if (!operation->WaitingForAssignmentsSince()) {
            operation->WaitingForAssignmentsSince() = Now_;
        }

        YT_TLOG_DEBUG("Planning full-host module-bound operation")
            .With("Module", operation->SchedulingModule())
            .With("ReadyToAssignGroupedNeededResources", operation->ReadyToAssignGroupedNeededResources())
            .With("WaitingForAssignmentsSince", operation->WaitingForAssignmentsSince())
            .With("OperationId", operation->GetId());

        YT_VERIFY(operation->SchedulingModule());

        // TODO(eshcherbin): (!) Deal with modules that can change between updates.
        auto& moduleState = GetOrCrash(ModuleStates_, *operation->SchedulingModule());

        // NB(severovv): Be careful, allocationGroupResources are modified during planning.
        for (const auto& [allocationGroupName, allocationGroupResources] : operation->ReadyToAssignGroupedNeededResources()) {
            // First we try to schedule allocations without preemption.
            PlanAllocationGroup(
                operation,
                allocationGroupName,
                allocationGroupResources,
                &moduleState.AvailableNodes(),
                EGpuAssignmentPlanningStage::FullHostModuleBound);

            // Then we try to schedule allocations using regular preemption.
            if (operation->IsStarving()) {
                PlanAllocationGroupWithPreemption(
                    operation,
                    allocationGroupName,
                    allocationGroupResources,
                    &moduleState.AvailableNodes(),
                    EGpuAssignmentPlanningStage::FullHostModuleBound);
            }

            // Finally, we try to schedule allocations using full-host aggressive preemption.
            if (ShouldUseFullHostAggressivePreemption(operation)) {
                PlanAllocationGroupWithPreemption(
                    operation,
                    allocationGroupName,
                    allocationGroupResources,
                    &moduleState.AvailableNodes(),
                    EGpuAssignmentPlanningStage::FullHostModuleBound,
                    /*useFullHostAggressivePreemption*/ true);
            }
        }

        if (operation->GetReadyToAssignNeededAllocationCount() == 0) {
            operation->WaitingForAssignmentsSince().reset();
        }
    }
}

THashMap<std::string, int> TGpuAllocationAssignmentPlanUpdateExecutor::DistributeAssignmentCountBetweenModules(const TAllocationGroupResources& resources) const
{
    THashMap<std::string, int> assignmentDistribution;
    int assignmentsLeft = resources.AllocationCount;
    if (assignmentsLeft == 0) {
        return assignmentDistribution;
    }

    std::vector<std::pair<int, std::string>> modulesBySize;
    for (const auto& [module, state] : ModuleStates_) {
        if (state.GetUnreservedNodeCount() > 0) {
            modulesBySize.emplace_back(state.GetUnreservedNodeCount(), module);
        }
    }
    std::ranges::sort(modulesBySize);

    for (const auto& [size, module] : modulesBySize) {
        int toReserve = std::min(size, assignmentsLeft);
        EmplaceOrCrash(assignmentDistribution, module, toReserve);
        assignmentsLeft -= toReserve;

        if (assignmentsLeft == 0) {
            break;
        }
    }

    return assignmentDistribution;
}

void TGpuAllocationAssignmentPlanUpdateExecutor::ProcessFullHostNonGangOperations()
{
    // 1. Initialize.
    std::vector<TOperationPtr> operationsToPlan;
    for (const auto& [_, operation] : Operations_) {
        if (!operation->IsFullHostNonGang()) {
            continue;
        }

        if (operation->GetReadyToAssignNeededAllocationCount() > 0) {
            operationsToPlan.push_back(operation);
        }
    }

    if (operationsToPlan.empty()) {
        return;
    }

    YT_TLOG_DEBUG("Planning full-host non-gang operations")
        .With("Count", std::ssize(operationsToPlan));

    // 2. Sort operations.
    std::ranges::sort(
        operationsToPlan,
        [&] (const TOperationPtr& lhs, const TOperationPtr& rhs) {
            bool lhsVanilla = lhs->GetType() == EOperationType::Vanilla;
            bool rhsVanilla = rhs->GetType() == EOperationType::Vanilla;
            if (lhsVanilla != rhsVanilla) {
                return lhsVanilla;
            }

            return lhs->GetReadyToAssignNeededAllocationCount() > rhs->GetReadyToAssignNeededAllocationCount();
        });

    // 3. Plan assignments.
    for (const auto& operation : operationsToPlan) {
        if (!operation->WaitingForAssignmentsSince()) {
            operation->WaitingForAssignmentsSince() = Now_;
        }

        // NB(severovv): Here we get allocationGroupResources by value, therefore allocationCount needs to be decreased manually.
        for (auto [allocationGroupName, allocationGroupResources] : operation->ReadyToAssignGroupedNeededResources()) {
            for (const auto& [module, assignmentCount] : DistributeAssignmentCountBetweenModules(allocationGroupResources)) {
                YT_TLOG_DEBUG("Planning full-host non-gang assignments for module")
                    .With("OperationId", operation->GetId())
                    .With("AllocationGroupName", allocationGroupName)
                    .With("Module", module)
                    .With("AssignmentCount", assignmentCount);

                auto& moduleState = GetOrCrash(ModuleStates_, module);
                allocationGroupResources.AllocationCount = assignmentCount;

                auto processPlannedAssignments = [&] (const std::vector<TAssignmentPtr>& assignments) {
                    for (const auto& assignment : assignments) {
                        YT_VERIFY(assignment->Node->SchedulingModule() == module);
                        moduleState.AddAssignment(assignment);
                    }
                    allocationGroupResources.AllocationCount -= std::ssize(assignments);
                };

                processPlannedAssignments(PlanAllocationGroup(
                    operation,
                    allocationGroupName,
                    allocationGroupResources,
                    &moduleState.AvailableNodes(),
                    EGpuAssignmentPlanningStage::FullHostNonGang));

                if (operation->IsStarving()) {
                    processPlannedAssignments(PlanAllocationGroupWithPreemption(
                        operation,
                        allocationGroupName,
                        allocationGroupResources,
                        &moduleState.AvailableNodes(),
                        EGpuAssignmentPlanningStage::FullHostNonGang,
                        /*useFullHostAggressivePreemption*/ false));
                }

                if (ShouldUseFullHostAggressivePreemption(operation)) {
                    processPlannedAssignments(PlanAllocationGroupWithPreemption(
                        operation,
                        allocationGroupName,
                        allocationGroupResources,
                        &moduleState.AvailableNodes(),
                        EGpuAssignmentPlanningStage::FullHostNonGang,
                        /*useFullHostAggressivePreemption*/ true));
                }
            }
        }

        if (operation->GetReadyToAssignNeededAllocationCount() == 0) {
            operation->WaitingForAssignmentsSince().reset();
        }
    }
}

void TGpuAllocationAssignmentPlanUpdateExecutor::SortFullHostModuleBoundOperations(std::vector<TOperationPtr>& operations)
{
    auto comparator = [&] (const TOperationPtr& lhs, const TOperationPtr& rhs) {
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

bool TGpuAllocationAssignmentPlanUpdateExecutor::ShouldUseFullHostAggressivePreemption(const TOperationPtr& operation) const
{
    return operation->IsFullHost() &&
        operation->WaitingForAssignmentsSince() &&
        *operation->WaitingForAssignmentsSince() + Config_->FullHostAggressivePreemptionTimeout < Now_;
}

bool TGpuAllocationAssignmentPlanUpdateExecutor::ShouldUsePriorityModuleBinding(const TOperationPtr& operation) const
{
    return operation->PriorityModuleBindingEnabled().value_or(false) &&
        operation->WaitingForModuleBindingSince() &&
        *operation->WaitingForModuleBindingSince() + Config_->PriorityModuleBindingTimeout < Now_;
}

bool TGpuAllocationAssignmentPlanUpdateExecutor::ShouldResetModule(const TOperationPtr& operation) const
{
    return operation->SchedulingModule() &&
        operation->WaitingForAssignmentsSince() &&
        operation->WaitingForAssignmentsSince().value() +
            Config_.GetModuleReconsiderationTimeout(*operation->SchedulingModule()) < Now_;
}

void TGpuAllocationAssignmentPlanUpdateExecutor::EvictReservation(
    const NDetail::TModuleReservationPtr& reservation,
    const std::string& preemptionDescription,
    const std::string& evictionModule)
{
    int preemptedAssignments = 0;
    auto& moduleState = GetOrCrash(ModuleStates_, evictionModule);

    Visit(reservation->GetItem(),
        [&] (const TOperationPtr& operation) {
            YT_VERIFY(evictionModule == operation->SchedulingModule());

            preemptedAssignments += std::ssize(operation->Assignments());
            operation->ResetSchedulingModule();
            PreemptAllOperationAssignments(
                operation,
                EAllocationPreemptionReason::EvictionFromSchedulingModule,
                preemptionDescription);
        },
        [&] (const TAssignmentPtr& assignment) {
            YT_VERIFY(evictionModule == assignment->Node->SchedulingModule());

            ++preemptedAssignments;
            Context_->PreemptAssignment(assignment, EAllocationPreemptionReason::EvictionFromSchedulingModule, preemptionDescription);
        });

    moduleState.RemoveReservation(reservation);
    Context_->GetStatistics()->PreemptedAssignmentsByStage[EGpuAssignmentPlanningStage::FullHostModuleBound] += preemptedAssignments;
}

void TGpuAllocationAssignmentPlanUpdateExecutor::EvictOperationFromSchedulingModule(const TOperationPtr& operation, const std::string& preemptionDescription)
{
    YT_VERIFY(operation->SchedulingModule());

    auto& moduleState = GetOrCrash(ModuleStates_, *operation->SchedulingModule());
    auto reservation = GetOrCrash(moduleState.FullHostBoundOperationReservations(), operation.Get());
    EvictReservation(reservation, preemptionDescription, *operation->SchedulingModule());
}

bool TGpuAllocationAssignmentPlanUpdateExecutor::BindFullHostOperationToModule(
    const TOperationPtr& operation,
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

    YT_TLOG_DEBUG("Trying to bind a full-host operation to a module")
        .With("AllModules", GetKeys(ModuleStates_))
        .With("SpecifiedModules", operation->SpecifiedSchedulingModules())
        .With("FeasibleModules", feasibleModules)
        .With("OperationUsedModule", operationUsedModule)
        .With("AllocationCount", allocationCount)
        .With("WaitingForModuleBindingSince", operation->WaitingForModuleBindingSince())
        .With("PriorityModuleBinding", priorityModuleBinding)
        .With("PriorityModuleBindingDeadline", priorityModuleBinding
            ? std::optional{Now_ + Config_->PriorityModuleBindingTimeout}
            : std::nullopt)
        .With("OperationId", operation->GetId());

    std::vector<std::pair<NDetail::TOperationModuleBindingOutcome, std::string>> possibleModuleBindings;
    for (const auto& module : feasibleModules) {
        if (auto outcome = ConsiderModuleForFullHostOperation(operation, module, priorityModuleBinding)) {
            YT_TLOG_DEBUG("Possible module binding outcome")
                .With("Module", module)
                .With("Outcome", *outcome)
                .With("OperationId", operation->GetId());

            possibleModuleBindings.emplace_back(std::move(*outcome), module);
        }
    }

    if (possibleModuleBindings.empty()) {
        YT_TLOG_DEBUG("Failed to choose a suitable module for operation")
            .With("OperationId", operation->GetId());

        if (!operation->WaitingForModuleBindingSince()) {
            operation->WaitingForModuleBindingSince() = Now_;
        }

        return false;
    }

    const auto& [bestModuleBindingOutcome, bestModule] = *std::ranges::min_element(possibleModuleBindings);

    LogStructuredGpuEventFluently(EGpuSchedulingLogEventType::OperationBoundToModule)
        .Item("operation_id").Value(operation->GetId())
        .Item("module").Value(bestModule);

    YT_TLOG_DEBUG("Binding full-host operation to module")
        .With("Module", bestModule)
        .With("OperationId", operation->GetId());

    if (operationUsedModule && (*operationUsedModule != bestModule)) {
        YT_TLOG_DEBUG("Preempting all operation's assignments in other module")
            .With("OperationUsedModule", operationUsedModule)
            .With("OperationId", operation->GetId());

        // NB(eshcherbin): This operation will not have the full ready to assign allocation count on this iteration,
        // so it will not be fully assigned. However, on the next iteration everything will be alright.
        PreemptAllOperationAssignments(
            operation,
            EAllocationPreemptionReason::OperationBoundToOtherModule,
            Format("Preempted because operation was bound to other scheduling module %v", bestModule));
    }

    for (const auto& evictedReservation : bestModuleBindingOutcome.ReservationsToEvict) {
        YT_TLOG_DEBUG("Evicting reservation from module in favour of a priority operation")
            .With("Module", bestModule)
            .With("Reservation", *evictedReservation)
            .With("PriorityOperationId", operation->GetId());

        EvictReservation(
            evictedReservation,
            Format("Preempted due to eviction from scheduling module in favour of priority operation %v", operation->GetId()),
            bestModule);
    }

    operation->WaitingForModuleBindingSince().reset();
    operation->SchedulingModule() = bestModule;

    auto& moduleState = GetOrCrash(ModuleStates_, bestModule);
    moduleState.AddFullHostBoundOperation(operation.Get());

    UpdateNetworkPriority(operation);

    return true;
}

void TGpuAllocationAssignmentPlanUpdateExecutor::UpdateNetworkPriority(const TOperationPtr& operation)
{
    YT_VERIFY(operation->SchedulingModule());

    const auto& moduleState = GetOrCrash(ModuleStates_, *operation->SchedulingModule());
    const int nodeCount = moduleState.GetNodeCount();
    if (nodeCount <= 0) {
        operation->NetworkPriority().reset();
        return;
    }

    // NB(yaishenka): GetInitialNeededAllocationCount is the canonical "node footprint on a module"
    // signal in this policy — see BindFullHostOperationToModule and FindOperationsToEvict.
    const auto share = static_cast<double>(operation->GetInitialNeededAllocationCount()) / nodeCount;
    operation->NetworkPriority() = ComputeNetworkPriority(
        share,
        Config_.GetModuleShareToNetworkPriority(*operation->SchedulingModule()));
}

std::optional<NDetail::TOperationModuleBindingOutcome> TGpuAllocationAssignmentPlanUpdateExecutor::ConsiderModuleForFullHostOperation(
    const TOperationPtr& operation,
    const std::string& module,
    bool priorityModuleBinding) const
{
    const auto& moduleState = GetOrCrash(ModuleStates_, module);
    const int allocationCount = operation->GetInitialNeededAllocationCount();

    YT_TLOG_DEBUG("Considering module for full-host operation")
        .With("Module", module)
        .With("ModuleState", moduleState)
        .With("AllocationCount", allocationCount)
        .With("OperationId", operation->GetId());

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
        std::vector<NDetail::TModuleReservation*> availableForEvictionReservations;
        for (const auto& reservation : moduleState.ModuleReservations()) {
            if (!reservation->IsPriorityModuleBoundOperation()) {
                availableForEvictionReservations.push_back(reservation.Get());
            }
        }

        int freedNodeCount;
        std::vector<NDetail::TModuleReservation*> reservationsToEvict;
        bool success = FindReservationsToEvict(
            availableForEvictionReservations,
            /*neededNodeCount*/ allocationCount - availableNodeCount,
            &reservationsToEvict,
            &freedNodeCount);
        if (success) {
            return NDetail::TOperationModuleBindingOutcome{
                .RemainingUnreservedNodeCount = availableNodeCount + freedNodeCount - allocationCount,
                .TotalEvictionPenalty = freedNodeCount,
                .ReservationsToEvict = std::move(reservationsToEvict),
            };
        }
    }

    return {};
}

//! This greedy algorithm finds a subset of |availableOperations| to evict,
//! such that the total freed node count exceeds |neededNodeCount|.
//! If total node count reserved for all available operations is not enough, returns false.
bool TGpuAllocationAssignmentPlanUpdateExecutor::FindReservationsToEvict(
    const std::vector<NDetail::TModuleReservation*>& availableReservations,
    int neededNodeCount,
    std::vector<NDetail::TModuleReservation*>* reservationsToEvict,
    int* freedNodeCount) const
{
    auto getReservedNodeCount = [] (const NDetail::TModuleReservation* reservation) {
        return reservation->GetNodeCount();
    };

    auto willSatisfyNeededNodeCountAfterReservation = [&] (const NDetail::TModuleReservation* reservation) {
        return *freedNodeCount + getReservedNodeCount(reservation) >= neededNodeCount;
    };

    *reservationsToEvict = availableReservations;
    std::ranges::sort(
        *reservationsToEvict,
        /*comp*/ std::greater{},
        /*proj*/ getReservedNodeCount);

    *freedNodeCount = 0;
    auto currentReservationIt = begin(*reservationsToEvict);
    while (currentReservationIt != reservationsToEvict->end() &&
        !willSatisfyNeededNodeCountAfterReservation(*currentReservationIt))
    {
        *freedNodeCount += getReservedNodeCount(*currentReservationIt);
        ++currentReservationIt;
    }

    if (currentReservationIt == reservationsToEvict->end()) {
        return false;
    }

    auto lastAddedReservationIt = currentReservationIt;
    while (currentReservationIt != reservationsToEvict->end() &&
        willSatisfyNeededNodeCountAfterReservation(*currentReservationIt))
    {
        *lastAddedReservationIt = std::move(*currentReservationIt);
        ++currentReservationIt;
    }

    *freedNodeCount += getReservedNodeCount(*lastAddedReservationIt);

    reservationsToEvict->erase(std::ranges::next(lastAddedReservationIt), end(*reservationsToEvict));

    return true;
}

void TGpuAllocationAssignmentPlanUpdateExecutor::ProcessRegularOperations()
{
    // 1. Initialize.
    std::vector<TOperationPtr> operationsToPlan;
    for (const auto& [_, operation] : Operations_) {
        if (operation->IsFullHost()) {
            continue;
        }

        if (operation->GetReadyToAssignNeededAllocationCount() > 0) {
            operationsToPlan.push_back(operation);
        }
    }

    if (operationsToPlan.empty()) {
        return;
    }

    YT_TLOG_DEBUG("Planning non full-host operations")
        .With("Count", std::ssize(operationsToPlan));

    // 2. Sort operations.
    std::ranges::sort(
        operationsToPlan,
        [&] (const TOperationPtr& lhs, const TOperationPtr& rhs) {
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
        // NB(severovv): Be careful, allocationGroupResources are modified during planning.
        for (const auto& [allocationGroupName, allocationGroupResources] : operation->ReadyToAssignGroupedNeededResources()) {
            PlanAllocationGroup(
                operation,
                allocationGroupName,
                allocationGroupResources,
                &SchedulableNodes_,
                EGpuAssignmentPlanningStage::Normal);

            if (operation->IsStarving()) {
                PlanAllocationGroupWithPreemption(
                    operation,
                    allocationGroupName,
                    allocationGroupResources,
                    &SchedulableNodes_,
                    EGpuAssignmentPlanningStage::Normal);
            }
        }
    }
}

void TGpuAllocationAssignmentPlanUpdateExecutor::ProcessRegularOperationsWithExtraResources()
{
    // 1. Initialize.
    std::vector<TOperationPtr> operationsToPlan;
    for (const auto& [_, operation] : Operations_ ) {
        if (operation->IsFullHost()) {
            continue;
        }

        if (operation->GetExtraNeededAllocationCount() > 0) {
            operationsToPlan.push_back(operation);
        }
    }

    if (operationsToPlan.empty()) {
        return;
    }

    YT_TLOG_DEBUG("Planning non gang operations with extra resources")
        .With("Count", std::ssize(operationsToPlan));

    // 2. Sort operations.
    // TODO(yaishenka): YT-26812 schedule jobs with extra resources (above fair share) more evenly.
    std::ranges::sort(
        operationsToPlan,
        [&] (const TOperationPtr& lhs, const TOperationPtr& rhs) {
            // Usually, vanilla operations are used for model training and map operations are used for batch inference.
            bool lhsVanilla = lhs->GetType() == EOperationType::Vanilla;
            bool rhsVanilla = rhs->GetType() == EOperationType::Vanilla;
            if (lhsVanilla != rhsVanilla) {
                return lhsVanilla;
            }

            // Operations with bigger allocations are processed first.
            const auto& lhsAllocationResources = lhs->ExtraGroupedNeededResources().begin()->second.MinNeededResources;
            const auto& rhsAllocationResources = rhs->ExtraGroupedNeededResources().begin()->second.MinNeededResources;
            if (lhsAllocationResources.GetGpu() != rhsAllocationResources.GetGpu()) {
                return lhsAllocationResources.GetGpu() > rhsAllocationResources.GetGpu();
            }

            // Finally, the bigger operation is, the sooner we want to process it.
            return lhs->GetExtraNeededAllocationCount() > rhs->GetExtraNeededAllocationCount();
        });

    // 3. Plan assignments.
    for (const auto& operation : operationsToPlan) {
        // NB(severovv): Be careful, allocationGroupResources are modified during planning.
        for (const auto& [allocationGroupName, allocationGroupResources] : operation->ExtraGroupedNeededResources()) {
            PlanPreemptibleAllocationGroup(
                operation,
                allocationGroupName,
                allocationGroupResources,
                &SchedulableNodes_,
                EGpuAssignmentPlanningStage::WithExtraResources);
        }
    }
}

void TGpuAllocationAssignmentPlanUpdateExecutor::PreemptAllOperationAssignments(
    const TOperationPtr& operation,
    EAllocationPreemptionReason preemptionReason,
    const std::string& preemptionDescription)
{
    // NB(eshcherbin): Copy assignments with |GetItems|, because the set will be modified.
    for (const auto& assignment : GetItems(operation->Assignments())) {
        Context_->PreemptAssignment(assignment, preemptionReason, preemptionDescription);
    }
}

std::vector<TAssignmentPtr> TGpuAllocationAssignmentPlanUpdateExecutor::PlanAllocationGroup(
    const TOperationPtr& operation,
    const std::string& allocationGroupName,
    TAllocationGroupResources allocationGroupResources,
    std::vector<TNode*>* availableNodes,
    EGpuAssignmentPlanningStage stage)
{
    if (allocationGroupResources.AllocationCount == 0) {
        return {};
    }

    YT_TLOG_DEBUG("Planning allocation group for operation")
        .WithFormat("AllocationGroup", "{Name: %v, Resources: %v}", allocationGroupName, allocationGroupResources)
        .With("OperationId", operation->GetId());

    TAllocationGroupPlanner planner(
        operation,
        allocationGroupName,
        allocationGroupResources,
        availableNodes,
        Context_,
        Logger);
    planner.Run();

    Context_->GetStatistics()->PlannedAssignmentsByStage[stage] += planner.GetPlannedAssignmentCount();

    YT_TLOG_DEBUG("Finished planning allocation group for operation")
        .With("PlannedAssignmentCount", planner.GetPlannedAssignmentCount())
        .WithFormat("AllocationGroup", "{Name: %v, Resources: %v}", allocationGroupName, allocationGroupResources)
        .With("OperationId", operation->GetId());

    return planner.PlannedAssignments();
}

std::vector<TAssignmentPtr> TGpuAllocationAssignmentPlanUpdateExecutor::PlanAllocationGroupWithPreemption(
    const TOperationPtr& operation,
    const std::string& allocationGroupName,
    TAllocationGroupResources allocationGroupResources,
    std::vector<TNode*>* availableNodes,
    EGpuAssignmentPlanningStage stage,
    bool useFullHostAggressivePreemption)
{
    if (allocationGroupResources.AllocationCount == 0) {
        return {};
    }

    YT_TLOG_DEBUG("Planning allocation group for operation with preemption")
        .WithFormat("AllocationGroup", "{Name: %v, Resources: %v}", allocationGroupName, allocationGroupResources)
        .With("UseFullHostAggressivePreemption", useFullHostAggressivePreemption)
        .With("OperationId", operation->GetId());

    TPreemptiveAllocationGroupPlanner planner(
        operation,
        allocationGroupName,
        allocationGroupResources,
        availableNodes,
        useFullHostAggressivePreemption,
        Context_,
        Config_.GetConfig(),
        Now_,
        Logger);
    planner.Run();

    Context_->GetStatistics()->PlannedAssignmentsByStage[stage] += planner.GetPlannedAssignmentCount();
    Context_->GetStatistics()->PreemptedAssignmentsByStage[stage] += planner.GetPreemptedAssignmentCount();

    YT_TLOG_DEBUG("Finished planning allocation group for operation with preemption")
        .With("PlannedAssignmentCount", planner.GetPlannedAssignmentCount())
        .With("PreemptedAssignmentCount", planner.GetPreemptedAssignmentCount())
        .WithFormat("AllocationGroup", "{Name: %v, Resources: %v}", allocationGroupName, allocationGroupResources)
        .With("OperationId", operation->GetId());

    return planner.PlannedAssignments();
}

int TGpuAllocationAssignmentPlanUpdateExecutor::GetLimitedAllocationCount(
    const TOperationPtr& operation,
    const std::string& allocationGroupName,
    const TAllocationGroupResources& allocationGroupResources) const
{
    if (allocationGroupResources.AllocationCount == 0) {
        return 0;
    }

    int limitedAllocationCount = allocationGroupResources.AllocationCount;
    auto maxAvailableResources = Context_->GetAvailableOperationLimits(operation);
    if (maxAvailableResources != TJobResources::Infinite()) {
        double maxAvailableAllocationCount = NVectorHdrf::GetMinResourceRatio(
            maxAvailableResources,
            allocationGroupResources.MinNeededResources);

        limitedAllocationCount = std::min(limitedAllocationCount, static_cast<int>(maxAvailableAllocationCount));
    }
    if (limitedAllocationCount != allocationGroupResources.AllocationCount) {
        YT_TLOG_DEBUG("Preemptible allocation group count decreased to satisfy limits")
            .WithFormat("AllocationGroup", "{Name: %v, Resources: %v}", allocationGroupName, allocationGroupResources)
            .With("OperationId", operation->GetId())
            .With("NonLimitedAllocationCount", allocationGroupResources.AllocationCount)
            .With("LimitedAllocationCount", limitedAllocationCount);
    }
    return limitedAllocationCount;
}

std::vector<TAssignmentPtr> TGpuAllocationAssignmentPlanUpdateExecutor::PlanPreemptibleAllocationGroup(
    const TOperationPtr& operation,
    const std::string& allocationGroupName,
    TAllocationGroupResources allocationGroupResources,
    std::vector<TNode*>* availableNodes,
    EGpuAssignmentPlanningStage stage)
{
    allocationGroupResources.AllocationCount = GetLimitedAllocationCount(operation, allocationGroupName, allocationGroupResources);

    if (allocationGroupResources.AllocationCount == 0) {
        return {};
    }

    YT_TLOG_DEBUG("Planning preemptible allocation group for operation")
        .WithFormat("AllocationGroup", "{Name: %v, Resources: %v}", allocationGroupName, allocationGroupResources)
        .With("OperationId", operation->GetId());

    TAllocationGroupPlanner planner(
        operation,
        allocationGroupName,
        allocationGroupResources,
        availableNodes,
        Context_,
        Logger,
        /*preemptible*/ true);
    planner.Run();

    Context_->GetStatistics()->PlannedAssignmentsByStage[stage] += planner.GetPlannedAssignmentCount();

    YT_TLOG_DEBUG("Finished planning preemptible allocation group for operation")
        .With("PlannedAssignmentCount", planner.GetPlannedAssignmentCount())
        .WithFormat("AllocationGroup", "{Name: %v, Resources: %v}", allocationGroupName, allocationGroupResources)
        .With("OperationId", operation->GetId());

    return planner.PlannedAssignments();
}

void TGpuAllocationAssignmentPlanUpdateExecutor::DumpModuleStatistics() const
{
    for (const auto& [module, moduleState] : ModuleStates_) {
        auto& moduleCounters = Context_->GetStatistics()->ModuleStatistics[module];
        moduleCounters.TotalNodes = moduleState.GetNodeCount();
        moduleCounters.UnreservedNodes = moduleState.GetUnreservedNodeCount();
        moduleCounters.FullHostModuleBoundOperations = std::ssize(moduleState.FullHostBoundOperationReservations());
        moduleCounters.FullHostNonGangAssignments = moduleState.GetFullHostNonGangAssignmentCount();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
