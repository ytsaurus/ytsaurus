#include "gpu_allocation_scheduler.h"

#include "private.h"
#include "persistent_fair_share_tree_allocation_scheduler_state.h"
#include "fair_share_tree_scheduling_snapshot.h"
#include "fair_share_tree_snapshot.h"
#include "fair_share_tree.h"

#include <util/generic/algorithm.h>

#include <yt/yt/core/logging/fluent_log.h>

namespace NYT::NScheduler {

using namespace NLogging;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

TGpuSchedulingContext::TGpuSchedulingContext(
    TGpuAllocationSchedulerHost* schedulerHost,
    TGpuSchedulerOperationStateMap* operationStates,
    TGpuSchedulerNodeStateMap* nodeStates,
    TInstant now,
    NLogging::TLogger logger,
    TGpuAllocationSchedulerConfigPtr config)
    : Now_(std::move(now))
    , Logger(logger)
    , Config_(std::move(config))
    , SchedulerHost_(schedulerHost)
    , OperationStates_(operationStates)
    , NodeStates_(nodeStates)
{ }

void TGpuSchedulingContext::SetNodeUsage(const TGpuSchedulerNodeStatePtr& node, const TJobResources& usage)
{
    SortedNodeStates_->erase(node);
    node->ResourceUsage = usage;
    SortedNodeStates_->insert(node);
}

bool TGpuSchedulingContext::CanSatisfyDiskQuotaRequests(
    const TDiskResources& diskResources,
    const THashMap<TGpuAllocationStatePtr, TDiskQuota>& diskQuotaRequests)
{
    std::vector<TDiskQuota> diskRequestsVector;
    for (const auto& [_, diskQuotaRequest] : diskQuotaRequests) {
        diskRequestsVector.push_back(diskQuotaRequest);
    }
    return ::NYT::NScheduler::CanSatisfyDiskQuotaRequests(diskResources, diskRequestsVector, /*considerUsage*/ true);
}

void TGpuSchedulingContext::ResetOperationModule(const TGpuSchedulerOperationStatePtr& operation)
{
    SchedulerHost_->ResetOperationModule(operation);

    double operationFairResourceAmount = operation->RuntimeAttributes()->FairResourceAmount;
    RemainingCapacityPerModule_[operation->SchedulingModule()] += operationFairResourceAmount;

    for (auto& [_, operations] : LargeOperationsToSchedulePerModule_) {
        operations.erase(operation);
    }
    SchedulerHost_->RemoveOperationFromNodes(operation);
}

void TGpuSchedulingContext::ScheduleAllocations()
{
    PrepareGpuSchedulingContext();
    AssignOperationsToModules();
    PrepareNodeStatesInGpuSchedulingContext();
    ScheduleLargeAllocationsToNodes();
    ScheduleSmallAllocationsToNodes();
}

void TGpuSchedulingContext::PrepareGpuSchedulingContext()
{
    for (auto& [nodeId, node] : *NodeStates_) {
        if (!node->Descriptor) {
            continue;
        }

        auto nodeModule = SchedulerHost_->GetNodeModule(*node);
        if (!nodeModule) {
            continue;
        }

        auto limit = node->Descriptor->ResourceLimits.GetGpu();
        TotalCapacityPerModule_[nodeModule] += limit;
    }
    RemainingCapacityPerModule_ = TotalCapacityPerModule_;

    for (auto& [operationId, operation] : *OperationStates_) {
        if (!IsOperationReady(operation)) {
            continue;
        }

        if (const auto& module = operation->SchedulingModule()) {
            RemainingCapacityPerModule_[*module] -= operation->RuntimeAttributes()->FairResourceAmount;
        }

        if (!operation->ScheduledAllocations().empty() &&
            operation->GetNeededResources() <= NVectorHdrf::RatioComparisonPrecision)
        {
            // Operation is fully scheduled.
            continue;
        }

        if (IsLargeOperation(operation)) {
            if (const auto& module = operation->SchedulingModule()) {
                YT_LOG_DEBUG("Operation is ready for scheduling in module "
                    "(OperationId: %v, SchedulingModule: %v)",
                    operationId,
                    *module);

                LargeOperationsToSchedulePerModule_[*module].insert(operation);
            } else {
                YT_LOG_DEBUG("Operation is ready for assigning to module "
                    "(OperationId: %v)",
                    operationId);

                LargeOperationsToAssign_.push_back(operation);
            }
        } else {
            YT_LOG_DEBUG("Operation is ready for scheduling "
                "(OperationId: %v)",
                operationId);

            SmallOperationsToSchedule_.insert(operation);
        }
    }

    YT_LOG_INFO("Gpu scheduling context prepared "
        "(TotalCapacityPerModule %v, "
        "RemainingCapacityPerModule %v, "
        "LargeOperationsToAssignCount: %v, "
        "SmallOperationsToScheduleCount: %v, )",
        TotalCapacityPerModule_,
        RemainingCapacityPerModule_,
        LargeOperationsToAssign_.size(),
        SmallOperationsToSchedule_.size());
}

void TGpuSchedulingContext::AssignOperationsToModules()
{
    std::sort(
        LargeOperationsToAssign_.begin(),
        LargeOperationsToAssign_.end(),
        [&] (const TGpuSchedulerOperationStatePtr& lhs, const TGpuSchedulerOperationStatePtr& rhs) {
            if (lhs->GetOperationHasPriority() != rhs->GetOperationHasPriority()) {
                return lhs->GetOperationHasPriority();
            }

            auto lhsSpecifiedModuleCount = lhs->SpecifiedSchedulingModules()
                ? lhs->SpecifiedSchedulingModules()->size()
                : Config_->GetModules().size();
            auto rhsSpecifiedModuleCount = rhs->SpecifiedSchedulingModules()
                ? rhs->SpecifiedSchedulingModules()->size()
                : Config_->GetModules().size();
            if (lhsSpecifiedModuleCount != rhsSpecifiedModuleCount) {
                return lhsSpecifiedModuleCount < rhsSpecifiedModuleCount;
            }

            return lhs->RuntimeAttributes()->Demand.GetGpu() > rhs->RuntimeAttributes()->Demand.GetGpu();
        });

    for (auto& operationState : LargeOperationsToAssign_) {
        auto operationDemand = operationState->RuntimeAttributes()->Demand.GetGpu();

        std::function<bool(double, double)> isModuleBetter;
        double initialBestRemainingCapacity;
        switch (Config_->ModuleAssignmentHeuristic) {
            case ESchedulingModuleAssignmentHeuristic::MaxRemainingCapacity:
                isModuleBetter = [] (double remainingCapacity, double bestRemainingCapacity) {
                    return bestRemainingCapacity < remainingCapacity;
                };
                initialBestRemainingCapacity = std::numeric_limits<double>::lowest();
                break;

            case ESchedulingModuleAssignmentHeuristic::MinRemainingFeasibleCapacity:
                isModuleBetter = [operationDemand] (double remainingCapacity, double bestRemainingCapacity) {
                    return remainingCapacity >= operationDemand && bestRemainingCapacity > remainingCapacity;
                };
                initialBestRemainingCapacity = std::numeric_limits<double>::max();
                break;
        }

        TSchedulingModule bestModule;
        auto bestRemainingCapacity = initialBestRemainingCapacity;
        const auto& specifiedModules = operationState->SpecifiedSchedulingModules();
        for (const auto& SchedulingModule : Config_->GetModules()) {
            auto it = RemainingCapacityPerModule_.find(SchedulingModule);
            auto remainingCapacity = it != RemainingCapacityPerModule_.end() ? it->second : 0.0;

            if (specifiedModules && !specifiedModules->contains(SchedulingModule)) {
                continue;
            }

            if (isModuleBetter(remainingCapacity, bestRemainingCapacity)) {
                bestModule = SchedulingModule;
                bestRemainingCapacity = remainingCapacity;
            }
        }

        if (!bestModule && operationState->GetOperationHasPriority()) {
            if (auto operationsToPreempt = FindBestOperationsToPreempt(operationState)) {
                PreemptNonPriorityOperationsFromModuleForOperation(operationState->OperationId(), operationsToPreempt->Operations);
                bestModule = operationsToPreempt->Module;
            }
        }

        if (!bestModule) {
            LogStructuredGpuEventFluently(EGpuSchedulingLogEventType::FailedToAssignOperation)
                .Item("operation_id").Value(operationState->OperationId())
                .Item("specified_modules").Value(operationState->SpecifiedSchedulingModules())
                .Item("remaining_capacity_per_module").Value(RemainingCapacityPerModule_)
                .Item("total_capacity_per_module").Value(TotalCapacityPerModule_);

            YT_LOG_INFO(
                "Failed to find a suitable module for operation "
                "(AvailableModules: %v, SpecifiedModules: %v, OperationDemand: %v, "
                "RemainingCapacityPerModule: %v, TotalCapacityPerModule: %v, OperationId: %v)",
                Config_->GetModules(),
                operationState->SpecifiedSchedulingModules(),
                operationDemand,
                RemainingCapacityPerModule_,
                TotalCapacityPerModule_,
                operationState->OperationId());

            if (!operationState->FailingToAssignToModuleSince()) {
                operationState->FailingToAssignToModuleSince() = Now_;
            }

            SetOperationEligibleForPriorityModuleAssignment(operationState);
            continue;
        }

        operationState->SchedulingModule() = bestModule;
        RemainingCapacityPerModule_[operationState->SchedulingModule()] -= operationDemand;

        operationState->FailingToAssignToModuleSince().reset();

        LargeOperationsToSchedulePerModule_[*bestModule].insert(operationState);
    }
}

void TGpuSchedulingContext::PrepareNodeStatesInGpuSchedulingContext()
{
    YT_VERIFY(!SortedNodeStates_);

    TGpuSchedulingContext::TNodeComparator nodeComparator =
        [&] (const TGpuSchedulerNodeStatePtr& lhs, const TGpuSchedulerNodeStatePtr& rhs) -> bool {
            auto lhsHasDescriptor = lhs->Descriptor != nullptr;
            auto rhsHasDescriptor = rhs->Descriptor != nullptr;
            if (lhsHasDescriptor != rhsHasDescriptor) {
                return lhsHasDescriptor;
            }

            auto lshResources = lhs->Descriptor->ResourceLimits - lhs->ResourceUsage;
            auto rshResources = rhs->Descriptor->ResourceLimits - rhs->ResourceUsage;

            if (lshResources.GetGpu() != rshResources.GetGpu()) {
                return lshResources.GetGpu() > rshResources.GetGpu();
            }

            return lhs->NodeId < rhs->NodeId;
        };

    SortedNodeStates_ = std::set<TGpuSchedulerNodeStatePtr, TGpuSchedulingContext::TNodeComparator>(nodeComparator);

    for (const auto& [nodeId, node] : *NodeStates_) {
        node->PreemptibleResourceUsage = TJobResources();
        node->PreemptibleAllocations.clear();
        for (auto allocation : node->RunningAllocations) {
            if (allocation->Preemptible) {
                node->PreemptibleAllocations.insert(allocation);
                node->PreemptibleResourceUsage += allocation->Resources;
            }
        }
        SortedNodeStates_->insert(node);
    }
}

void TGpuSchedulingContext::ScheduleLargeAllocationsToNodes()
{
    for (auto& [module, operations] : LargeOperationsToSchedulePerModule_) {
        DoScheduleAllocationsToNodes(operations);
    }
}

void TGpuSchedulingContext::ScheduleSmallAllocationsToNodes()
{
    DoScheduleAllocationsToNodes(SmallOperationsToSchedule_);
}

void TGpuSchedulingContext::DoScheduleAllocationsToNodes(
    const THashSet<TGpuSchedulerOperationStatePtr>& operations)
{
    std::vector<TGpuSchedulerOperationStatePtr> operationStates(
        operations.begin(),
        operations.end());

    std::sort(
        operationStates.begin(),
        operationStates.end(),
        [&] (const TGpuSchedulerOperationStatePtr& lhs, const TGpuSchedulerOperationStatePtr& rhs) {
            return CompareGpuSchedulerOperationStatesForPreSchedulingSort(lhs, rhs);
        });

    for (auto& operation : operationStates) {
        UpdateOperationAllocationsToSchedule(operation);

        auto allocationToSchedule = operation->AllocationsToSchedule().begin();

        THashMap<TNodeId, TNodeWithSchedulingInfo> suitableNodes;
        while (allocationToSchedule != operation->AllocationsToSchedule().end()) {
            YT_VERIFY(!operation->RuntimeAttributes()->AllocationResources.empty());

            auto allocationResources = (*allocationToSchedule)->Resources;

            auto suitableNode = FindBestSuitableNodeForAllocation(
                operation,
                *allocationToSchedule,
                suitableNodes);

            if (!suitableNode.Node) {
                break;
            }

            auto& suitableNodeInfo = suitableNodes[suitableNode.Node->NodeId];
            suitableNodeInfo.Node = suitableNode.Node;
            suitableNodeInfo.Allocations.push_back(*allocationToSchedule);
            suitableNodeInfo.ScheduledResources += allocationResources;
            suitableNodeInfo.AllocationsToPreempt = suitableNode.AllocationsToPreempt;

            ++allocationToSchedule;
        }

        // NB(omgronny): Do not schedule large operations partially.
        if (IsLargeOperation(operation) && allocationToSchedule != operation->AllocationsToSchedule().end()) {
            YT_LOG_INFO("Operation allocations are partially scheduled "
                "(OperationId: %v, AllocationsToScheduleCount: %v)",
                operation->OperationId(),
                operation->AllocationsToSchedule().size());

            if (!operation->FailingToScheduleAtModuleSince()) {
                operation->FailingToScheduleAtModuleSince() = Now_;
            }
            continue;
        }
        operation->FailingToScheduleAtModuleSince().reset();

        YT_LOG_INFO("Operation allocations are fully scheduled "
            "(OperationId: %v, ScheduledAllocationsCount: %v, NodeCount: %v)",
            operation->OperationId(),
            operation->AllocationsToSchedule().size(),
            suitableNodes.size());

        for (auto& [nodeId, suitableNode] : suitableNodes) {
            auto& node = suitableNode.Node;

            SortedNodeStates_->erase(node);

            for (auto allocation : suitableNode.AllocationsToPreempt) {
                SchedulerHost_->RemoveAllocationFromNode(
                    allocation,
                    node);
            }

            for (auto allocation : suitableNode.Allocations) {
                operation->OnAllocationScheduled(allocation, node->NodeId);

                node->ResourceUsage += allocation->Resources.ToJobResources();
                if (allocation->Resources.DiskQuota() != TDiskQuota()) {
                    EmplaceOrCrash(node->DiskRequests, allocation, allocation->Resources.DiskQuota());
                }
                node->RunningAllocations.insert(allocation);
            }

            // NB(omgronny): Update node order.
            SortedNodeStates_->insert(node);
        }
    }
}

TGpuSchedulingContext::TBestNodeForAllocation TGpuSchedulingContext::FindBestSuitableNodeForAllocation(
    const TGpuSchedulerOperationStatePtr& operation,
    const TGpuAllocationStatePtr& allocation,
    const THashMap<TNodeId, TNodeWithSchedulingInfo>& scheduledResourcesPerNode)
{
    const auto& allocationResources = allocation->Resources;

    TBestNodeForAllocation result;
    for (auto& node : *SortedNodeStates_) {
        if (!node->Descriptor) {
            continue;
        }

        if (auto nodeModule = SchedulerHost_->GetNodeModule(*node);
            operation->SchedulingModule() &&
            (!nodeModule || *nodeModule != *operation->SchedulingModule()))
        {
            continue;
        }

        auto nodeInfo = GetOrDefault(scheduledResourcesPerNode, node->NodeId);
        auto nodeUsage = node->ResourceUsage + nodeInfo.ScheduledResources.ToJobResources();

        auto diskRequests = node->DiskRequests;
        if (auto quota = nodeInfo.ScheduledResources.DiskQuota() + allocationResources.DiskQuota();
            quota != TDiskQuota())
        {
            diskRequests[allocation] = quota;
        }
        bool nodeCanSatisfyDiskQuotaRequests = CanSatisfyDiskQuotaRequests(node->Descriptor->DiskResources, diskRequests);

        if (Dominates(node->Descriptor->ResourceLimits - nodeUsage, allocationResources.ToJobResources()) &&
            nodeCanSatisfyDiskQuotaRequests)
        {
            result = TBestNodeForAllocation{
                .Node = node,
            };
            continue;
        }

        // NB(omgronny): We have already scheduled large allocation to this node.
        if (!nodeInfo.Allocations.empty()) {
            continue;
        }

        // NB(omgronny): Use scheduling with preemption only if needed.
        if (result.Node) {
            return result;
        }

        // NB(omgronny): Ignore disk usage.
        if (Dominates(node->Descriptor->ResourceLimits - nodeUsage + node->PreemptibleResourceUsage, allocationResources.ToJobResources()) &&
            nodeCanSatisfyDiskQuotaRequests)
        {
            result = TBestNodeForAllocation{
                .Node = node,
                .AllocationsToPreempt = node->PreemptibleAllocations,
            };
            continue;
        }

        bool canPreemptSmallAllocationsForLarge = operation->FailingToScheduleAtModuleSince() &&
            *operation->FailingToScheduleAtModuleSince() + Config_->PreemptForLargeOperationTimeout < Now_;

        if (IsLargeOperation(operation) && IsNodeWithSmallAllocations(node) && canPreemptSmallAllocationsForLarge) {
            // NB(omgronny): Ignore disk usage.
            if (Dominates(
                node->Descriptor->ResourceLimits,
                allocationResources.ToJobResources()))
            {
                result = TBestNodeForAllocation{
                    .Node = node,
                    .AllocationsToPreempt = node->RunningAllocations,
                };
                continue;
            }
        }
    }

    return result;
}

bool TGpuSchedulingContext::IsNodeWithSmallAllocations(
    const TGpuSchedulerNodeStatePtr& node) const
{
    const auto& allocations = node->RunningAllocations;

    if (allocations.empty()) {
        return false;
    }

    auto& operation = GetOrCrash(*OperationStates_, (*allocations.begin())->OperationId);

    return !IsLargeOperation(operation);
}

void TGpuSchedulingContext::UpdateOperationAllocationsToSchedule(const TGpuSchedulerOperationStatePtr& operation)
{
    operation->AllocationsToSchedule().clear();
    auto neededResources = operation->GetNeededResources();

    YT_LOG_DEBUG("Update operation allocations to schedule "
        "(OperationId: %v, NeededResources: %v)",
        operation->OperationId(),
        neededResources);

    for (const auto& allocationState : operation->RuntimeAttributes()->AllocationResources) {
        auto resource = allocationState->Resources.GetGpu();
        if (neededResources >= resource && !allocationState->Scheduled) {
            neededResources -= resource;
            operation->AllocationsToSchedule().insert(allocationState);
        }
    }

    YT_LOG_DEBUG("Operation allocations to schedule updated "
        "(OperationId: %v, AllocationsToScheduleCount: %v)",
        operation->OperationId(),
        operation->AllocationsToSchedule().size());
}

void TGpuSchedulingContext::SetOperationEligibleForPriorityModuleAssignment(
    const TGpuSchedulerOperationStatePtr& operation)
{
    const auto& runtimeAttributes = operation->RuntimeAttributes();
    YT_VERIFY(runtimeAttributes);

    auto failingToAssignToModuleSince = operation->FailingToAssignToModuleSince();

    auto operationHasPriority = runtimeAttributes->EffectivePrioritySchedulingModuleAssignmentEnabled &&
        failingToAssignToModuleSince &&
        Now_ > *failingToAssignToModuleSince + Config_->PriorityModuleAssignmentTimeout;
    operation->SetOperationHasPriority(operationHasPriority);
}

void TGpuSchedulingContext::PreemptNonPriorityOperationsFromModuleForOperation(
    TOperationId priorityOperationId,
    const std::vector<TGpuSchedulerOperationStatePtr>& operations)
{
    for (auto& operation : operations) {
        auto module = operation->SchedulingModule();

        ResetOperationModule(operation);

        YT_LOG_DEBUG(
            "Operation preempted from module "
            "(OperationId: %v, "
            "Module: %v, "
            "RemainingCapacityPerModule: %v, "
            "PriorityOperationId: %v)",
            operation->OperationId(),
            module,
            RemainingCapacityPerModule_,
            priorityOperationId);
    }
}

std::optional<TGpuSchedulingContext::TOperationsToPreempt> TGpuSchedulingContext::FindBestOperationsToPreempt(
    const TGpuSchedulerOperationStatePtr& operation)
{
    if (!operation->RuntimeAttributes()) {
        return {};
    }
    const auto& runtimeAttributes = *operation->RuntimeAttributes();

    std::optional<TOperationsToPreempt> bestOperationsToPreempt;
    const auto& specifiedModules = operation->SpecifiedSchedulingModules();
    for (const auto& module : Config_->GetModules()) {
        if (specifiedModules && !specifiedModules->contains(module)) {
            continue;
        }

        auto neededDemand = runtimeAttributes.Demand.GetGpu() - RemainingCapacityPerModule_[module];
        if (neededDemand <= 0.0) {
            return TOperationsToPreempt{
                .Module = module,
            };
        }

        auto bestOperationsToPreemptInModule = [&] {
            switch (Config_->ModulePreemptionHeuristic) {
                case ESchedulingModulePreemptionHeuristic::Greedy:
                    return FindBestOperationsToPreemptInModuleGreedy(module, neededDemand);
            }
        }();

        if (!bestOperationsToPreemptInModule) {
            continue;
        }

        if (!bestOperationsToPreempt ||
            bestOperationsToPreemptInModule->TotalPenalty < bestOperationsToPreempt->TotalPenalty)
        {
            bestOperationsToPreempt = std::move(bestOperationsToPreemptInModule);
        }
    }

    if (!bestOperationsToPreempt) {
        YT_LOG_INFO(
            "Failed to find a suitable operation set in any module to preempt for a priority operation "
            "(OperationId: %v)",
            operation->OperationId());

        return {};
    }

    YT_LOG_INFO(
        "Found operations to preempt for a priority operation "
        "(OperationId: %v, BestOperationsToPreemptSize: %v, TotalPenalty: %v)",
        operation->OperationId(),
        bestOperationsToPreempt->Operations.size(),
        bestOperationsToPreempt->TotalPenalty);

    return bestOperationsToPreempt;
}

std::optional<TGpuSchedulingContext::TOperationsToPreempt> TGpuSchedulingContext::FindBestOperationsToPreemptInModuleGreedy(
    const TSchedulingModule& module,
    double neededDemand)
{
    YT_VERIFY(module);

    std::vector<TGpuSchedulerOperationStatePtr> assignedOperationElements;
    assignedOperationElements.reserve(OperationStates_->size());
    for (const auto& [_, operation] : *OperationStates_) {
        auto operationScheduledToModule = !operation->ScheduledAllocations().empty() &&
            operation->SchedulingModule() == module;
        if (LargeOperationsToSchedulePerModule_[*module].contains(operation) ||
            operationScheduledToModule)
        {
            assignedOperationElements.push_back(operation);
        }
    }
    std::sort(
        assignedOperationElements.begin(),
        assignedOperationElements.end(),
        [&] (const TGpuSchedulerOperationStatePtr& lhs, const TGpuSchedulerOperationStatePtr& rhs) {
            return lhs->RuntimeAttributes()->FairResourceAmount > rhs->RuntimeAttributes()->FairResourceAmount;
        });

    std::vector<TGpuSchedulerOperationStatePtr> bestOperationsToPreempt;
    double fairResourceAmount = 0.0;
    auto currentCandidate = assignedOperationElements.begin();
    while (currentCandidate != assignedOperationElements.end() && (*currentCandidate)->GetOperationHasPriority()) {
        ++currentCandidate;
    }

    while (fairResourceAmount + NVectorHdrf::RatioComparisonPrecision < neededDemand &&
        currentCandidate != assignedOperationElements.end())
    {
        auto nextCandidate = std::next(currentCandidate);

        while (nextCandidate != assignedOperationElements.end()) {
            auto nextCandidateFairResourceAmount = (*nextCandidate)->RuntimeAttributes()->FairResourceAmount;
            if (nextCandidateFairResourceAmount + fairResourceAmount + NVectorHdrf::RatioComparisonPrecision < neededDemand)
            {
                break;
            }

            currentCandidate = nextCandidate;
            nextCandidate = std::next(currentCandidate);
        }

        bestOperationsToPreempt.push_back(*currentCandidate);
        fairResourceAmount += (*currentCandidate)->RuntimeAttributes()->FairResourceAmount;
        ++currentCandidate;
    }

    if (fairResourceAmount + NVectorHdrf::RatioComparisonPrecision < neededDemand) {
        return {};
    }

    return TOperationsToPreempt{
        .TotalPenalty = fairResourceAmount,
        .Operations = std::move(bestOperationsToPreempt),
        .Module = module,
    };
}

bool TGpuSchedulingContext::CompareGpuSchedulerOperationStatesForPreSchedulingSort(
    const TGpuSchedulerOperationStatePtr& lhs,
    const TGpuSchedulerOperationStatePtr& rhs) const
{
    if (lhs->SchedulingModule().has_value() !=
        rhs->SchedulingModule().has_value())
    {
        return lhs->SchedulingModule().has_value();
    }

    if (lhs->GetOperationHasPriority() != rhs->GetOperationHasPriority()) {
        return lhs->GetOperationHasPriority();
    }

    // TODO: Remove this check?
    if (lhs->GetIsGang() != rhs->GetIsGang()) {
        return lhs->GetIsGang();
    }

    const auto& lhsMinNeededResources = lhs->AggregatedInitialMinNeededResources();
    const auto& rhsMinNeededResources = rhs->AggregatedInitialMinNeededResources();
    if (lhsMinNeededResources.has_value() != rhsMinNeededResources.has_value()) {
        return lhsMinNeededResources.has_value();
    }

    if (lhsMinNeededResources->GetGpu() != rhsMinNeededResources->GetGpu()) {
        return lhsMinNeededResources->GetGpu() > rhsMinNeededResources->GetGpu();
    }

    if (lhs->GetNeededResources() != rhs->GetNeededResources()) {
        return lhs->GetNeededResources() > rhs->GetNeededResources();
    }

    return lhs->OperationId() < rhs->OperationId();
}

bool TGpuSchedulingContext::IsLargeOperation(const TGpuSchedulerOperationStatePtr& operation) const
{
    bool isGangMultiHostOperation = operation->GetIsGang() &&
        operation->RuntimeAttributes()->Demand.GetGpu() > LargeGpuAllocationGpuDemand;

    bool isFullHostOperation = operation->AggregatedInitialMinNeededResources() &&
        operation->AggregatedInitialMinNeededResources()->GetGpu() == LargeGpuAllocationGpuDemand &&
        operation->RuntimeAttributes()->Demand.GetGpu() == LargeGpuAllocationGpuDemand;

    return isGangMultiHostOperation || isFullHostOperation;
}

bool TGpuSchedulingContext::IsOperationReady(const TGpuSchedulerOperationStatePtr& operation) const
{
    if (!operation->RuntimeAttributes()) {
        return false;
    }

    if (!operation->AggregatedInitialMinNeededResources()) {
        return false;
    }

    const auto& runtimeAttributes = *operation->RuntimeAttributes();

    // NB(eshcherbin): Demand could be zero, because needed resources update is asynchronous.
    if (runtimeAttributes.Demand == TJobResources()) {
        return false;
    }

    if (runtimeAttributes.AllocationResources.empty()) {
        return false;
    }

    if (!operation->GetIsGang()) {
        return true;
    }

    if (runtimeAttributes.FairResourceAmount < runtimeAttributes.Demand.GetGpu()) {
        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

TGpuAllocationScheduler::TGpuAllocationScheduler(
    IInvokerPtr invoker,
    TGpuAllocationSchedulerConfigPtr config,
    NLogging::TLogger logger)
    : Invoker_(std::move(invoker))
    , Logger(std::move(logger))
    , Config_(std::move(config))
{ }

void TGpuAllocationScheduler::RegisterOperation(
    TOperationId operationId,
    bool isGang,
    std::optional<THashSet<TString>> specifiedSchedulingModules)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    YT_LOG_INFO("Registering operation "
        "(OperationId: %v, IsGang: %v)",
        operationId,
        isGang);

    auto operation = New<TGpuSchedulerOperationState>(operationId, isGang, specifiedSchedulingModules);

    OperationStates_.emplace(operationId, operation);
}

void TGpuAllocationScheduler::UnregisterOperation(TOperationId operationId)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    YT_LOG_INFO("Unregistering operation "
        "(OperationId: %v)",
        operationId);

    auto& operation = GetOrCrash(OperationStates_, operationId);

    RemoveOperationFromNodes(operation);

    OperationStates_.erase(operationId);
}

void TGpuAllocationScheduler::RegisterNode(TNodeId nodeId, const TFairShareTreeAllocationSchedulerNodeState& node)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    YT_LOG_INFO("Registering node "
        "(NodeId: %v)",
        nodeId);

    auto nodeState = New<TGpuSchedulerNodeState>();
    nodeState->NodeId = nodeId;
    nodeState->Descriptor = node.Descriptor;

    EmplaceOrCrash(NodeStates_, nodeId, nodeState);
}

void TGpuAllocationScheduler::UnregisterNode(TNodeId nodeId)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    YT_LOG_INFO("Unregistering node "
        "(NodeId: %v)",
        nodeId);

    auto& node = GetOrCrash(NodeStates_, nodeId);

    RemoveAllAllocationsFromNode(node);

    NodeStates_.erase(nodeId);
}

// TODO: Persistent state.
// TODO: More structured logging.
void TGpuAllocationScheduler::ScheduleAllocations()
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    auto now = TInstant::Now();

    ResetOperationModuleAssignments(now);

    TGpuSchedulingContext context(
        this,
        &OperationStates_,
        &NodeStates_,
        now,
        Logger,
        Config_);

    context.ScheduleAllocations();

    // TODO: Profiling and etc.
}

void TGpuAllocationScheduler::ResetOperationModuleAssignments(TInstant now)
{
    for (auto& [operationId, operation] : OperationStates_) {
        if (!operation->RuntimeAttributes()) {
            continue;
        }
        const auto& runtimeAttributes = *operation->RuntimeAttributes();

        auto& schedulingModule = operation->SchedulingModule();
        if (!schedulingModule) {
            continue;
        }

        auto& failingToRunAllocationsAtModuleSince = operation->FailingToRunAllocationsAtModuleSince();
        if (runtimeAttributes.UsageAtUpdate != runtimeAttributes.Demand) {
            if (!failingToRunAllocationsAtModuleSince) {
                failingToRunAllocationsAtModuleSince = now;
            }

            if (*failingToRunAllocationsAtModuleSince + Config_->ModuleReconsiderationTimeout < now) {
                YT_LOG_INFO(
                    "Operation has failed to schedule all allocations for too long, revoking its module assignment "
                    "(OperationId: %v, PreviousModule: %v, ResourceUsage: %v, ResourceDemand: %v, Timeout: %v)",
                    operationId,
                    schedulingModule,
                    runtimeAttributes.UsageAtUpdate,
                    runtimeAttributes.Demand,
                    Config_->ModuleReconsiderationTimeout);

                ResetOperationModule(operation);
                continue;
            }
        } else {
            failingToRunAllocationsAtModuleSince.reset();
        }

        bool hasZeroUsageAndFairShare = (runtimeAttributes.UsageAtUpdate == TJobResources()) &&
            (runtimeAttributes.FairResourceAmount <= NVectorHdrf::RatioComparisonPrecision);
        if (hasZeroUsageAndFairShare) {
            YT_LOG_DEBUG(
                "Revoking operation module assignment because it has zero fair share and usage "
                "(OperationId: %v, PreviousModule: %v, ResourceUsage: %v, ResourceDemand: %v)",
                operationId,
                schedulingModule,
                runtimeAttributes.UsageAtUpdate,
                runtimeAttributes.Demand);

            ResetOperationModule(operation);
        }
    }
}

void TGpuAllocationScheduler::ResetOperationModule(const TGpuSchedulerOperationStatePtr& operation)
{
    auto& operationModule = operation->SchedulingModule();
    YT_VERIFY(operationModule);

    LogStructuredGpuEventFluently(EGpuSchedulingLogEventType::OperationModuleAssignmentRevoked)
        .Item("operation_id").Value(operation->OperationId())
        .Item("scheduling_module").Value(operationModule);

    operation->SchedulingModule().reset();
    operation->FailingToScheduleAtModuleSince().reset();
    operation->FailingToRunAllocationsAtModuleSince().reset();
}

void TGpuAllocationScheduler::UpdateConfig(TGpuAllocationSchedulerConfigPtr config)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    Config_ = std::move(config);
}

void TGpuAllocationScheduler::UpdateOperationRuntimeAttributes(TOperationId operationId, TOperationRuntimeAttributes attributes)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    auto& operation = GetOrCrash(OperationStates_, operationId);

    operation->RuntimeAttributes() = std::move(attributes);
}

void TGpuAllocationScheduler::UpdateOperationMinNeededResources(TOperationId operationId, TJobResources minNeededResources)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    auto& operation = GetOrCrash(OperationStates_, operationId);

    operation->AggregatedInitialMinNeededResources() = std::move(minNeededResources);
}

THashSet<TGpuAllocationStatePtr> TGpuAllocationScheduler::GetScheduledAllocationsForNode(NNodeTrackerClient::TNodeId nodeId) const
{
    return GetOrCrash(NodeStates_, nodeId)->RunningAllocations;
}

void TGpuAllocationScheduler::OnAllocationFinished(
    TOperationId operationId,
    const TGpuAllocationStatePtr& allocation,
    TNodeId nodeId)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    // NB(omgronny): Race with operation unregistering is possible.
    if (!OperationStates_.contains(operationId)) {
        return;
    }

    auto& operation = GetOrCrash(OperationStates_, operationId);
    YT_VERIFY(!operation->ScheduledAllocations().empty());

    auto& nodeState = GetOrCrash(NodeStates_, nodeId);
    RemoveAllocationFromNode(allocation, nodeState);
}

void TGpuAllocationScheduler::RemoveOperationFromNodes(const TGpuSchedulerOperationStatePtr& operation)
{
    YT_LOG_DEBUG("Removing operation from all nodes "
        "(OperationId: %v, ScheduledAllocationsCount: %v, ResourceUsage: %v)",
        operation->OperationId(),
        operation->GetResourceUsage(),
        operation->ScheduledAllocations().size());

    for (const auto& [allocationId, nodeId] : operation->ScheduledAllocations()) {
        auto& nodeState = GetOrCrash(NodeStates_, nodeId);
        RemoveAllocationFromNode(allocationId, nodeState);
    }
    YT_VERIFY(operation->GetResourceUsage() == 0.0);
}

void TGpuAllocationScheduler::RemoveAllocationFromNode(
    const TGpuAllocationStatePtr& allocation,
    const TGpuSchedulerNodeStatePtr& node)
{
    auto& operation = GetOrCrash(OperationStates_, allocation->OperationId);

    auto allocationResources = allocation->Resources;
    node->ResourceUsage -= allocationResources;
    node->DiskRequests.erase(allocation);

    node->RunningAllocations.erase(allocation);

    operation->RemoveAllocation(allocation);
}

void TGpuAllocationScheduler::RemoveAllAllocationsFromNode(
    const TGpuSchedulerNodeStatePtr& node)
{
    YT_LOG_DEBUG("Removing all operations from node "
        "(NodeId: %v, RunningAllocationsCount: %v, ResourceUsage: %v)",
        node->NodeId,
        node->RunningAllocations.size(),
        node->ResourceUsage);

    for (const auto& allocationState : node->RunningAllocations) {
        auto& operation = GetOrCrash(OperationStates_, allocationState->OperationId);

        operation->RemoveAllocation(allocationState);
    }
    node->RunningAllocations.clear();
}

const TSchedulingModule& TGpuAllocationScheduler::GetNodeModule(
    const std::optional<std::string>& nodeDataCenter,
    const std::optional<std::string>& nodeInfinibandCluster,
    ESchedulingModuleType moduleType) const
{
    switch (moduleType) {
        case ESchedulingModuleType::DataCenter:
            return nodeDataCenter;
        case ESchedulingModuleType::InfinibandCluster:
            return nodeInfinibandCluster;
    }
}

const TSchedulingModule& TGpuAllocationScheduler::GetNodeModule(
    const TExecNodeDescriptorPtr& nodeDescriptor,
    ESchedulingModuleType moduleType) const
{
    return GetNodeModule(nodeDescriptor->DataCenter, nodeDescriptor->InfinibandCluster, moduleType);
}

const TSchedulingModule& TGpuAllocationScheduler::GetNodeModule(const TGpuSchedulerNodeState& node) const
{
    YT_ASSERT(node.Descriptor);

    return GetNodeModule(node.Descriptor, Config_->ModuleType);
}

THashMap<NNodeTrackerClient::TNodeId, THashSet<TGpuAllocationStatePtr>> TGpuAllocationScheduler::GetScheduledAllocations() const
{
    THashMap<NNodeTrackerClient::TNodeId, THashSet<TGpuAllocationStatePtr>> result;
    for (auto& [nodeId, nodeState] : NodeStates_) {
        if (!nodeState->RunningAllocations.empty()) {
            result[nodeId] = nodeState->RunningAllocations;
        }
    }
    return result;
}

TGpuSchedulerNodeStatePtr TGpuAllocationScheduler::GetNodeState(NNodeTrackerClient::TNodeId nodeId) const
{
    return GetOrDefault(NodeStates_, nodeId);
}

TGpuSchedulerOperationStatePtr TGpuAllocationScheduler::GetOperationState(TOperationId operationId)
{
    return GetOrCrash(OperationStates_, operationId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
