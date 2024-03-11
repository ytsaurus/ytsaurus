#include "scheduling_segment_manager.h"

#include "private.h"
#include "persistent_fair_share_tree_allocation_scheduler_state.h"
#include "fair_share_tree_scheduling_snapshot.h"
#include "fair_share_tree_snapshot.h"

#include <util/generic/algorithm.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static constexpr int LargeGpuSegmentAllocationGpuDemand = 8;

////////////////////////////////////////////////////////////////////////////////

namespace {

double GetNodeResourceLimit(const TFairShareTreeAllocationSchedulerNodeState& node, EJobResourceType resourceType)
{
    return node.Descriptor->Online
        ? GetResource(node.Descriptor->ResourceLimits, resourceType)
        : 0.0;
}

EJobResourceType GetSegmentBalancingKeyResource(ESegmentedSchedulingMode mode)
{
    switch (mode) {
        case ESegmentedSchedulingMode::LargeGpu:
            return EJobResourceType::Gpu;
        default:
            YT_ABORT();
    }
}

TNodeMovePenalty GetMovePenaltyForNode(
    const TFairShareTreeAllocationSchedulerNodeState& node,
    ESegmentedSchedulingMode mode)
{
    auto keyResource = GetSegmentBalancingKeyResource(mode);
    switch (keyResource) {
        case EJobResourceType::Gpu:
            return TNodeMovePenalty{
                .PriorityPenalty = node.RunningAllocationStatistics.TotalGpuTime -
                    node.RunningAllocationStatistics.PreemptibleGpuTime,
                .RegularPenalty = node.RunningAllocationStatistics.TotalGpuTime
            };
        default:
            return TNodeMovePenalty{
                .PriorityPenalty = node.RunningAllocationStatistics.TotalCpuTime -
                    node.RunningAllocationStatistics.PreemptibleCpuTime,
                .RegularPenalty = node.RunningAllocationStatistics.TotalCpuTime
            };
    }
}

void SortByPenalty(TNodeWithMovePenaltyList& nodeWithPenaltyList)
{
    SortBy(nodeWithPenaltyList, [] (const TNodeWithMovePenalty& node) { return node.MovePenalty; });
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool operator <(const TNodeMovePenalty& lhs, const TNodeMovePenalty& rhs)
{
    if (lhs.PriorityPenalty != rhs.PriorityPenalty) {
        return lhs.PriorityPenalty < rhs.PriorityPenalty;
    }
    return lhs.RegularPenalty < rhs.RegularPenalty;
}

TNodeMovePenalty& operator +=(TNodeMovePenalty& lhs, const TNodeMovePenalty& rhs)
{
    lhs.PriorityPenalty += rhs.PriorityPenalty;
    lhs.RegularPenalty += rhs.RegularPenalty;
    return lhs;
}

void FormatValue(TStringBuilderBase* builder, const TNodeMovePenalty& penalty, TStringBuf /*format*/)
{
    builder->AppendFormat("{PriorityPenalty: %.3f, RegularPenalty: %.3f}",
        penalty.PriorityPenalty,
        penalty.RegularPenalty);
}

TString ToString(const TNodeMovePenalty& penalty)
{
    return ToStringViaBuilder(penalty);
}

////////////////////////////////////////////////////////////////////////////////

const TSchedulingSegmentModule& TSchedulingSegmentManager::GetNodeModule(
    const std::optional<TString>& nodeDataCenter,
    const std::optional<TString>& nodeInfinibandCluster,
    ESchedulingSegmentModuleType moduleType)
{
    switch (moduleType) {
        case ESchedulingSegmentModuleType::DataCenter:
            return nodeDataCenter;
        case ESchedulingSegmentModuleType::InfinibandCluster:
            return nodeInfinibandCluster;
        default:
            YT_ABORT();
    }
}

const TSchedulingSegmentModule& TSchedulingSegmentManager::GetNodeModule(
    const TExecNodeDescriptorPtr& nodeDescriptor,
    ESchedulingSegmentModuleType moduleType)
{
    return GetNodeModule(nodeDescriptor->DataCenter, nodeDescriptor->InfinibandCluster, moduleType);
}

TString TSchedulingSegmentManager::GetNodeTagFromModuleName(const TString& moduleName, ESchedulingSegmentModuleType moduleType)
{
    switch (moduleType) {
        case ESchedulingSegmentModuleType::DataCenter:
            return moduleName;
        case ESchedulingSegmentModuleType::InfinibandCluster:
            return Format("%v:%v", InfinibandClusterNameKey, moduleName);
        default:
            YT_ABORT();
    }
}

TSchedulingSegmentManager::TSchedulingSegmentManager(
    TString treeId,
    TFairShareStrategySchedulingSegmentsConfigPtr config,
    NLogging::TLogger logger,
    const NProfiling::TProfiler& profiler)
    : TreeId_(std::move(treeId))
    , Logger(std::move(logger))
    , Config_(std::move(config))
    , SensorProducer_(New<TBufferedProducer>())
{
    profiler.AddProducer("/segments", SensorProducer_);
}

void TSchedulingSegmentManager::UpdateSchedulingSegments(TUpdateSchedulingSegmentsContext* context)
{
    if (Config_->Mode != ESegmentedSchedulingMode::Disabled) {
        DoUpdateSchedulingSegments(context);
    } else if (PreviousMode_ != ESegmentedSchedulingMode::Disabled) {
        Reset(context);
    }

    PreviousMode_ = Config_->Mode;

    LogAndProfileSegments(context);
    BuildPersistentState(context);
}

void TSchedulingSegmentManager::InitOrUpdateOperationSchedulingSegment(
    TOperationId operationId,
    const TFairShareTreeAllocationSchedulerOperationStatePtr& operationState) const
{
    if (!operationState->AggregatedInitialMinNeededResources) {
        // May happen if we're updating segments config, and there's an operation that hasn't materialized yet.
        return;
    }

    auto segment = [&] {
        if (auto specifiedSegment = operationState->Spec->SchedulingSegment) {
            return *specifiedSegment;
        }

        switch (Config_->Mode) {
            case ESegmentedSchedulingMode::LargeGpu: {
                bool meetsGangCriterion = operationState->IsGang || !Config_->AllowOnlyGangOperationsInLargeSegment;
                auto allocationGpuDemand = operationState->AggregatedInitialMinNeededResources->GetGpu();
                bool meetsAllocationGpuDemandCriterion = (allocationGpuDemand == LargeGpuSegmentAllocationGpuDemand);
                return meetsGangCriterion && meetsAllocationGpuDemandCriterion
                    ? ESchedulingSegment::LargeGpu
                    : ESchedulingSegment::Default;
            }
            default:
                return ESchedulingSegment::Default;
        }
    }();

    if (operationState->SchedulingSegment != segment) {
        YT_LOG_INFO(
            "Setting new scheduling segment for operation ("
            "Segment: %v, Mode: %v, AllowOnlyGangOperationsInLargeSegment: %v, IsGang: %v, "
            "InitialMinNeededResources: %v, SpecifiedSegment: %v, OperationId: %v)",
            segment,
            Config_->Mode,
            Config_->AllowOnlyGangOperationsInLargeSegment,
            operationState->IsGang,
            operationState->AggregatedInitialMinNeededResources,
            operationState->Spec->SchedulingSegment,
            operationId);

        operationState->SchedulingSegment = segment;
        operationState->SpecifiedSchedulingSegmentModules = operationState->Spec->SchedulingSegmentModules;
        if (!IsModuleAwareSchedulingSegment(segment)) {
            operationState->SchedulingSegmentModule.reset();
        }
    }
}

void TSchedulingSegmentManager::UpdateConfig(TFairShareStrategySchedulingSegmentsConfigPtr config)
{
    Config_ = std::move(config);
}

void TSchedulingSegmentManager::DoUpdateSchedulingSegments(TUpdateSchedulingSegmentsContext* context)
{
    // Process operations.
    CollectCurrentResourceAmountPerSegment(context);
    CollectFairResourceAmountPerSegment(context);
    ResetOperationModuleAssignments(context);
    AssignOperationsToModules(context);

    // Process nodes.
    ValidateInfinibandClusterTags(context);
    ApplySpecifiedSegments(context);
    CheckAndRebalanceSegments(context);
}

void TSchedulingSegmentManager::Reset(TUpdateSchedulingSegmentsContext* context)
{
    PreviousMode_ = ESegmentedSchedulingMode::Disabled;
    UnsatisfiedSince_.reset();

    for (auto& [_, operation] : context->OperationStates) {
        operation->SchedulingSegmentModule.reset();
        operation->FailingToScheduleAtModuleSince.reset();
    }
    for (auto& [_, node] : context->NodeStates) {
        SetNodeSegment(&node, ESchedulingSegment::Default, context);
    }
}

void TSchedulingSegmentManager::ResetOperationModule(const TSchedulerOperationElement* operationElement, TUpdateSchedulingSegmentsContext* context) const
{
    auto& operation = context->OperationStates[operationElement->GetOperationId()];
    auto& operationModule = operation->SchedulingSegmentModule;
    YT_VERIFY(operationModule);

    const auto& segment = operation->SchedulingSegment;
    YT_VERIFY(segment);

    double operationFairResourceAmount = GetElementFairResourceAmount(operationElement, context);

    context->FairResourceAmountPerSegment.At(*segment).MutableAt(operationModule) -= operationFairResourceAmount;
    context->RemainingCapacityPerModule[operationModule] += operationFairResourceAmount;

    // NB: We will abort all allocations that are running in the wrong module.
    operation->SchedulingSegmentModule.reset();
    operation->FailingToScheduleAtModuleSince.reset();
}

void TSchedulingSegmentManager::PreemptNonPriorityOperationsFromModuleForOperation(
    TOperationId priorityOperationId,
    const TNonOwningOperationElementList& operations,
    TUpdateSchedulingSegmentsContext* context) const
{
    for (const auto* operationElement : operations) {
        auto module = context->OperationStates[operationElement->GetOperationId()]->SchedulingSegmentModule;

        ResetOperationModule(operationElement, context);

        YT_LOG_DEBUG(
            "Operation preempted from module "
            "(OperationId: %v, "
            "Module: %v, "
            "RemainingCapacityPerModule: %v, "
            "PriorityOperationId: %v)",
            operationElement->GetOperationId(),
            module,
            context->RemainingCapacityPerModule,
            priorityOperationId);
    }
}

bool TSchedulingSegmentManager::IsOperationEligibleForPriorityModuleAssigment(
    const TSchedulerOperationElement* operationElement,
    TUpdateSchedulingSegmentsContext* context) const
{
    const auto& treeSnapshot = context->TreeSnapshot;
    const auto& attributes = treeSnapshot->SchedulingSnapshot()->StaticAttributesList().AttributesOf(operationElement);
    const auto& operation = context->OperationStates[operationElement->GetOperationId()];
    auto failingToAssignToModuleSince = operation->FailingToAssignToModuleSince;

    return attributes.EffectivePrioritySchedulingSegmentModuleAssignmentEnabled &&
        failingToAssignToModuleSince &&
        context->Now > *failingToAssignToModuleSince + Config_->PriorityModuleAssignmentTimeout;
}

double TSchedulingSegmentManager::GetElementFairResourceAmount(const TSchedulerOperationElement* element, TUpdateSchedulingSegmentsContext* context) const
{
    auto keyResource = GetSegmentBalancingKeyResource(Config_->Mode);
    auto snapshotTotalKeyResourceLimit = GetResource(context->TreeSnapshot->ResourceLimits(), keyResource);
    return element->Attributes().FairShare.Total[keyResource] * snapshotTotalKeyResourceLimit;
}

THashMap<TSchedulingSegmentModule, TNonOwningOperationElementList> TSchedulingSegmentManager::CollectNonPriorityAssignedOperationsPerModule(
    TUpdateSchedulingSegmentsContext* context) const
{
    THashMap<TSchedulingSegmentModule, TNonOwningOperationElementList> assignedOperationsPerModule;

    const auto& treeSnapshot = context->TreeSnapshot;
    for (const auto& [operationId, operation] : context->OperationStates) {
        auto* element = context->TreeSnapshot->FindEnabledOperationElement(operationId);
        if (!element) {
            continue;
        }

        const auto& segment = operation->SchedulingSegment;
        if (!segment) {
            continue;
        }

        const auto& attributes = treeSnapshot->SchedulingSnapshot()->StaticAttributesList().AttributesOf(element);
        auto operationHasPriority = attributes.EffectivePrioritySchedulingSegmentModuleAssignmentEnabled;
        if (auto module = operation->SchedulingSegmentModule;
            module && !operationHasPriority)
        {
            assignedOperationsPerModule[module].push_back(element);
        }
    }

    return assignedOperationsPerModule;
}

std::optional<TSchedulingSegmentManager::TOperationsToPreempt> TSchedulingSegmentManager::FindBestOperationsToPreempt(
    TOperationId operationId,
    TUpdateSchedulingSegmentsContext* context) const
{
    auto element = context->TreeSnapshot->FindEnabledOperationElement(operationId);
    if (!element) {
        return {};
    }

    auto keyResource = GetSegmentBalancingKeyResource(Config_->Mode);
    auto operationDemand = GetResource(element->ResourceDemand(), keyResource);

    auto assignedOperationPerModule = CollectNonPriorityAssignedOperationsPerModule(context);

    std::optional<TOperationsToPreempt> bestOperationsToPreempt;
    const auto& specifiedModules = context->OperationStates[operationId]->SpecifiedSchedulingSegmentModules;
    for (const auto& module : Config_->GetModules()) {
        if (specifiedModules && !specifiedModules->contains(module)) {
            continue;
        }

        auto neededDemand = operationDemand - context->RemainingCapacityPerModule[module];
        if (neededDemand <= 0.0) {
            return TOperationsToPreempt{
                .Module = module,
            };
        }

        auto bestOperationsToPreemptInModule = [&] {
            switch (Config_->ModulePreemptionHeuristic) {
                case ESchedulingSegmentModulePreemptionHeuristic::Greedy:
                    return FindBestOperationsToPreemptInModuleGreedy(module, neededDemand, std::move(assignedOperationPerModule[module]), context);

                default:
                    YT_ABORT();
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
            operationId);

        return {};
    }

    YT_LOG_INFO(
        "Found operations to preempt for a priority operation "
        "(OperationId: %v, BestOperationsToPreemptSize: %v, TotalPenalty: %v)",
        operationId,
        bestOperationsToPreempt->Operations.size(),
        bestOperationsToPreempt->TotalPenalty);

    return bestOperationsToPreempt;
}

std::optional<TSchedulingSegmentManager::TOperationsToPreempt> TSchedulingSegmentManager::FindBestOperationsToPreemptInModuleGreedy(
    const TSchedulingSegmentModule& module,
    double neededDemand,
    TNonOwningOperationElementList assignedOperationElements,
    TUpdateSchedulingSegmentsContext* context) const
{
    std::sort(assignedOperationElements.begin(), assignedOperationElements.end(), [&] (const TSchedulerOperationElement* lhs, const TSchedulerOperationElement* rhs) {
        return GetElementFairResourceAmount(lhs, context) > GetElementFairResourceAmount(rhs, context);
    });

    TNonOwningOperationElementList bestOperationsToPreempt;
    double fairResourceAmount = 0.0;
    auto currentCandidate = assignedOperationElements.begin();
    while (fairResourceAmount + NVectorHdrf::RatioComparisonPrecision < neededDemand &&
        currentCandidate != assignedOperationElements.end())
    {
        auto nextCandidate = std::next(currentCandidate);

        while (nextCandidate != assignedOperationElements.end()) {
            auto nextCandidateFairResourceAmount = GetElementFairResourceAmount(*nextCandidate, context);
            if (nextCandidateFairResourceAmount + fairResourceAmount + NVectorHdrf::RatioComparisonPrecision < neededDemand)
            {
                break;
            }

            currentCandidate = nextCandidate;
            nextCandidate = std::next(currentCandidate);
        }

        bestOperationsToPreempt.push_back(*currentCandidate);
        fairResourceAmount += GetElementFairResourceAmount(*currentCandidate, context);
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

void TSchedulingSegmentManager::CollectCurrentResourceAmountPerSegment(TUpdateSchedulingSegmentsContext* context) const
{
    auto keyResource = GetSegmentBalancingKeyResource(Config_->Mode);
    for (const auto& [_, node] : context->NodeStates) {
        if (!node.Descriptor) {
            continue;
        }

        auto nodeKeyResourceLimit = GetNodeResourceLimit(node, keyResource);
        const auto& nodeModule = GetNodeModule(node);
        auto& currentResourceAmount = IsModuleAwareSchedulingSegment(node.SchedulingSegment)
            ? context->CurrentResourceAmountPerSegment.At(node.SchedulingSegment).MutableAt(nodeModule)
            : context->CurrentResourceAmountPerSegment.At(node.SchedulingSegment).Mutable();
        currentResourceAmount += nodeKeyResourceLimit;
        context->TotalCapacityPerModule[nodeModule] += nodeKeyResourceLimit;
        context->NodesTotalKeyResourceLimit += nodeKeyResourceLimit;
    }

    context->RemainingCapacityPerModule = context->TotalCapacityPerModule;
    auto snapshotTotalKeyResourceLimit = GetResource(context->TreeSnapshot->ResourceLimits(), keyResource);

    YT_LOG_WARNING_IF(context->NodesTotalKeyResourceLimit != snapshotTotalKeyResourceLimit,
        "Total key resource limit from node states differs from the tree snapshot, "
        "operation scheduling segments distribution might be unfair "
        "(NodesTotalKeyResourceLimit: %v, SnapshotTotalKeyResourceLimit: %v, KeyResource: %v)",
        context->NodesTotalKeyResourceLimit,
        snapshotTotalKeyResourceLimit,
        keyResource);
}

void TSchedulingSegmentManager::CollectFairResourceAmountPerSegment(TUpdateSchedulingSegmentsContext* context) const
{
    // First, collect fair share from operations that have been already assigned to modules.
    auto keyResource = GetSegmentBalancingKeyResource(Config_->Mode);
    for (auto& [operationId, operation] : context->OperationStates) {
        auto* element = context->TreeSnapshot->FindEnabledOperationElement(operationId);
        if (!element) {
            continue;
        }

        const auto& segment = operation->SchedulingSegment;
        if (!segment) {
            // Segment may be unset due to a race, and in this case we silently ignore the operation.
            continue;
        }

        auto& fairShareAtSegment = context->FairSharePerSegment.At(*segment);
        if (IsModuleAwareSchedulingSegment(*segment)) {
            fairShareAtSegment.MutableAt(operation->SchedulingSegmentModule) += element->Attributes().FairShare.Total[keyResource];
        } else {
            fairShareAtSegment.Mutable() += element->Attributes().FairShare.Total[keyResource];
        }
    }

    // Second, compute fair resource amounts.
    auto snapshotTotalKeyResourceLimit = GetResource(context->TreeSnapshot->ResourceLimits(), keyResource);
    for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
        if (IsModuleAwareSchedulingSegment(segment)) {
            for (const auto& schedulingSegmentModule : Config_->GetModules()) {
                auto fairResourceAmount =
                    context->FairSharePerSegment.At(segment).GetOrDefaultAt(schedulingSegmentModule) * snapshotTotalKeyResourceLimit;
                context->FairResourceAmountPerSegment.At(segment).SetAt(schedulingSegmentModule, fairResourceAmount);
                context->RemainingCapacityPerModule[schedulingSegmentModule] -= fairResourceAmount;
            }
        } else {
            auto fairResourceAmount = context->FairSharePerSegment.At(segment).GetOrDefault() * snapshotTotalKeyResourceLimit;
            context->FairResourceAmountPerSegment.At(segment).Set(fairResourceAmount);
        }
    }

    // Third, apply specified reserves.
    for (auto segment: TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
        if (IsModuleAwareSchedulingSegment(segment)) {
            for (const auto& schedulingSegmentModule : Config_->GetModules()) {
                auto reserve = std::min(
                    Config_->ReserveFairResourceAmount.At(segment).GetOrDefaultAt(schedulingSegmentModule),
                    context->RemainingCapacityPerModule[schedulingSegmentModule]);
                context->FairResourceAmountPerSegment.At(segment).MutableAt(schedulingSegmentModule) += reserve;
                context->RemainingCapacityPerModule[schedulingSegmentModule] -= reserve;
            }
        } else {
            auto reserve = Config_->ReserveFairResourceAmount.At(segment).GetOrDefault();
            context->FairResourceAmountPerSegment.At(segment).Mutable() += reserve;
        }
    }
}

void TSchedulingSegmentManager::ResetOperationModuleAssignments(TUpdateSchedulingSegmentsContext* context) const
{
    if (context->Now <= InitializationDeadline_) {
        return;
    }

    for (const auto& [operationId, operation] : context->OperationStates) {
        auto* element = context->TreeSnapshot->FindEnabledOperationElement(operationId);
        if (!element) {
            continue;
        }

        const auto& segment = operation->SchedulingSegment;
        if (!segment || !IsModuleAwareSchedulingSegment(*segment)) {
            // Segment may be unset due to a race, and in this case we silently ignore the operation.
            continue;
        }

        auto& schedulingSegmentModule = operation->SchedulingSegmentModule;
        if (!schedulingSegmentModule) {
            continue;
        }

        auto& failingToScheduleAtModuleSince = operation->FailingToScheduleAtModuleSince;
        if (element->ResourceUsageAtUpdate() != element->ResourceDemand()) {
            if (!failingToScheduleAtModuleSince) {
                failingToScheduleAtModuleSince = context->Now;
            }

            if (*failingToScheduleAtModuleSince + Config_->ModuleReconsiderationTimeout < context->Now) {
                YT_LOG_INFO(
                    "Operation has failed to schedule all allocations for too long, revoking its module assignment "
                    "(OperationId: %v, SchedulingSegment: %v, PreviousModule: %v, ResourceUsage: %v, ResourceDemand: %v, Timeout: %v, "
                    "InitializationDeadline: %v)",
                    operationId,
                    segment,
                    schedulingSegmentModule,
                    element->ResourceUsageAtUpdate(),
                    element->ResourceDemand(),
                    Config_->ModuleReconsiderationTimeout,
                    InitializationDeadline_);

                ResetOperationModule(element, context);
                continue;
            }
        } else {
            failingToScheduleAtModuleSince.reset();
        }

        bool hasZeroUsageAndFairShare = (element->ResourceUsageAtUpdate() == TJobResources()) &&
            Dominates(TResourceVector::SmallEpsilon(), element->Attributes().FairShare.Total);
        if (hasZeroUsageAndFairShare && Config_->EnableModuleResetOnZeroFairShareAndUsage) {
            YT_LOG_DEBUG(
                "Revoking operation module assignment because it has zero fair share and usage "
                "(OperationId: %v, SchedulingSegment: %v, PreviousModule: %v, ResourceUsage: %v, ResourceDemand: %v, "
                "InitializationDeadline: %v)",
                operationId,
                segment,
                schedulingSegmentModule,
                element->ResourceUsageAtUpdate(),
                element->ResourceDemand(),
                InitializationDeadline_);

            ResetOperationModule(element, context);
        }
    }
}

void TSchedulingSegmentManager::AssignOperationsToModules(TUpdateSchedulingSegmentsContext* context) const
{
    auto keyResource = GetSegmentBalancingKeyResource(Config_->Mode);

    struct TOperationStateWithElement
    {
        TOperationId OperationId;
        TFairShareTreeAllocationSchedulerOperationState* Operation;
        TSchedulerOperationElement* Element;
        bool OperationHasPriority;
    };
    std::vector<TOperationStateWithElement> operationsToAssignToModule;
    operationsToAssignToModule.reserve(context->OperationStates.size());
    for (auto& [operationId, operation] : context->OperationStates) {
        auto* element = context->TreeSnapshot->FindEnabledOperationElement(operationId);
        if (!element) {
            continue;
        }

        const auto& segment = operation->SchedulingSegment;
        if (!segment || !IsModuleAwareSchedulingSegment(*segment)) {
            continue;
        }

        // NB(eshcherbin): Demand could be zero, because needed resources update is asynchronous.
        if (element->ResourceDemand() == TJobResources()) {
            continue;
        }

        bool demandFullySatisfied = Dominates(element->Attributes().FairShare.Total + TResourceVector::Epsilon(), element->Attributes().DemandShare);
        if (operation->SchedulingSegmentModule || !demandFullySatisfied) {
            continue;
        }

        operationsToAssignToModule.push_back(TOperationStateWithElement{
            .OperationId = operationId,
            .Operation = operation.Get(),
            .Element = element,
            .OperationHasPriority = IsOperationEligibleForPriorityModuleAssigment(element, context),
        });
    }

    std::sort(
        operationsToAssignToModule.begin(),
        operationsToAssignToModule.end(),
        [&] (const TOperationStateWithElement& lhs, const TOperationStateWithElement& rhs) {
            if (lhs.OperationHasPriority != rhs.OperationHasPriority) {
                return lhs.OperationHasPriority;
            }

            auto lhsSpecifiedModuleCount = lhs.Operation->SpecifiedSchedulingSegmentModules
                ? lhs.Operation->SpecifiedSchedulingSegmentModules->size()
                : Config_->GetModules().size();
            auto rhsSpecifiedModuleCount = rhs.Operation->SpecifiedSchedulingSegmentModules
                ? rhs.Operation->SpecifiedSchedulingSegmentModules->size()
                : Config_->GetModules().size();
            if (lhsSpecifiedModuleCount != rhsSpecifiedModuleCount) {
                return lhsSpecifiedModuleCount < rhsSpecifiedModuleCount;
            }

            return GetResource(rhs.Element->ResourceDemand(), keyResource) <
                GetResource(lhs.Element->ResourceDemand(), keyResource);
        });

    for (const auto& [operationId, operation, element, operationHasPriority] : operationsToAssignToModule) {
        const auto& segment = operation->SchedulingSegment;
        auto operationDemand = GetResource(element->ResourceDemand(), keyResource);

        std::function<bool(double, double)> isModuleBetter;
        double initialBestRemainingCapacity;
        switch (Config_->ModuleAssignmentHeuristic) {
            case ESchedulingSegmentModuleAssignmentHeuristic::MaxRemainingCapacity:
                isModuleBetter = [] (double remainingCapacity, double bestRemainingCapacity) {
                    return bestRemainingCapacity < remainingCapacity;
                };
                initialBestRemainingCapacity = std::numeric_limits<double>::lowest();
                break;

            case ESchedulingSegmentModuleAssignmentHeuristic::MinRemainingFeasibleCapacity:
                isModuleBetter = [operationDemand] (double remainingCapacity, double bestRemainingCapacity) {
                    return remainingCapacity >= operationDemand && bestRemainingCapacity > remainingCapacity;
                };
                initialBestRemainingCapacity = std::numeric_limits<double>::max();
                break;

            default:
                YT_ABORT();
        }

        TSchedulingSegmentModule bestModule;
        auto bestRemainingCapacity = initialBestRemainingCapacity;
        const auto& specifiedModules = operation->SpecifiedSchedulingSegmentModules;
        for (const auto& schedulingSegmentModule : Config_->GetModules()) {
            auto it = context->RemainingCapacityPerModule.find(schedulingSegmentModule);
            auto remainingCapacity = it != context->RemainingCapacityPerModule.end() ? it->second : 0.0;

            if (specifiedModules && !specifiedModules->contains(schedulingSegmentModule)) {
                continue;
            }

            if (isModuleBetter(remainingCapacity, bestRemainingCapacity)) {
                bestModule = schedulingSegmentModule;
                bestRemainingCapacity = remainingCapacity;
            }
        }

        if (!bestModule && operationHasPriority) {
            if (auto operationsToPreempt = FindBestOperationsToPreempt(operationId, context)) {
                PreemptNonPriorityOperationsFromModuleForOperation(operationId, operationsToPreempt->Operations, context);
                bestModule = operationsToPreempt->Module;
            }
        }

        if (!bestModule) {
            YT_LOG_INFO(
                "Failed to find a suitable module for operation "
                "(AvailableModules: %v, SpecifiedModules: %v, OperationDemand: %v, "
                "RemainingCapacityPerModule: %v, TotalCapacityPerModule: %v, OperationId: %v)",
                Config_->GetModules(),
                operation->SpecifiedSchedulingSegmentModules,
                operationDemand,
                context->RemainingCapacityPerModule,
                context->TotalCapacityPerModule,
                operationId);

            if (!operation->FailingToAssignToModuleSince) {
                operation->FailingToAssignToModuleSince = context->Now;
            }

            continue;
        }

        auto operationFairShare = element->Attributes().FairShare.Total[keyResource];
        context->FairSharePerSegment.At(*segment).MutableAt(operation->SchedulingSegmentModule) -= operationFairShare;
        operation->SchedulingSegmentModule = bestModule;
        context->FairSharePerSegment.At(*segment).MutableAt(operation->SchedulingSegmentModule) += operationFairShare;

        context->FairResourceAmountPerSegment.At(*segment).MutableAt(operation->SchedulingSegmentModule) += operationDemand;
        context->RemainingCapacityPerModule[operation->SchedulingSegmentModule] -= operationDemand;

        operation->FailingToAssignToModuleSince.reset();

        YT_LOG_INFO(
            "Assigned operation to a new scheduling segment module "
            "(SchedulingSegment: %v, Module: %v, SpecifiedModules: %v, "
            "OperationDemand: %v, RemainingCapacityPerModule: %v, TotalCapacityPerModule: %v, "
            "OperationId: %v)",
            segment,
            operation->SchedulingSegmentModule,
            operation->SpecifiedSchedulingSegmentModules,
            operationDemand,
            context->RemainingCapacityPerModule,
            context->TotalCapacityPerModule,
            operationId);
    }
}

void TSchedulingSegmentManager::ValidateInfinibandClusterTags(TUpdateSchedulingSegmentsContext* context) const
{
    static const TString InfinibandClusterTagPrefix = InfinibandClusterNameKey + ":";

    if (!Config_->EnableInfinibandClusterTagValidation) {
        return;
    }

    auto validateNodeDescriptor = [&] (const TExecNodeDescriptor& node) -> TError {
        if (!node.InfinibandCluster || !Config_->InfinibandClusters.contains(*node.InfinibandCluster)) {
            return TError("Node's infiniband cluster is invalid or missing")
                << TErrorAttribute("node_infiniband_cluster", node.InfinibandCluster)
                << TErrorAttribute("configured_infiniband_clusters", Config_->InfinibandClusters);
        }

        std::vector<TString> infinibandClusterTags;
        for (const auto& tag : node.Tags.GetSourceTags()) {
            if (tag.StartsWith(InfinibandClusterTagPrefix)) {
                infinibandClusterTags.push_back(tag);
            }
        }

        if (infinibandClusterTags.empty()) {
            return TError("Node has no infiniband cluster tags");
        }

        if (std::ssize(infinibandClusterTags) > 1) {
            return TError("Node has more than one infiniband cluster tags")
                << TErrorAttribute("infiniband_cluster_tags", infinibandClusterTags);
        }

        const auto& tag = infinibandClusterTags[0];
        if (tag != TSchedulingSegmentManager::GetNodeTagFromModuleName(*node.InfinibandCluster, ESchedulingSegmentModuleType::InfinibandCluster)) {
            return TError("Node's infiniband cluster tag doesn't match its infiniband cluster from annotations")
                << TErrorAttribute("infiniband_cluster", node.InfinibandCluster)
                << TErrorAttribute("infiniband_cluster_tag", tag);
        }

        return {};
    };

    for (const auto& [nodeId, node] : context->NodeStates) {
        auto error = validateNodeDescriptor(*node.Descriptor);
        if (!error.IsOK()) {
            error = error << TErrorAttribute("node_address", node.Descriptor->Address);
            context->Error = TError("Node's infiniband cluster tags validation failed in tree %Qv", TreeId_)
                << std::move(error);
            break;
        }
    }
}

void TSchedulingSegmentManager::ApplySpecifiedSegments(TUpdateSchedulingSegmentsContext* context) const
{
    for (auto& [nodeId, node] : context->NodeStates) {
        if (auto segment = node.SpecifiedSchedulingSegment) {
            SetNodeSegment(&node, *segment, context);
        }
    }
}

void TSchedulingSegmentManager::CheckAndRebalanceSegments(TUpdateSchedulingSegmentsContext* context)
{
    auto [isSegmentUnsatisfied, hasUnsatisfiedSegment] = FindUnsatisfiedSegments(context);
    if (hasUnsatisfiedSegment) {
        if (!UnsatisfiedSince_) {
            UnsatisfiedSince_ = context->Now;
        }

        YT_LOG_DEBUG(
            "Found unsatisfied scheduling segments in tree "
            "(IsSegmentUnsatisfied: %v, UnsatisfiedFor: %v, Timeout: %v, InitializationDeadline: %v)",
            isSegmentUnsatisfied,
            context->Now - *UnsatisfiedSince_,
            Config_->UnsatisfiedSegmentsRebalancingTimeout,
            InitializationDeadline_);

        auto deadline = std::max(
            *UnsatisfiedSince_ + Config_->UnsatisfiedSegmentsRebalancingTimeout,
            InitializationDeadline_);
        if (context->Now > deadline) {
            DoRebalanceSegments(context);
            UnsatisfiedSince_.reset();
        }
    } else {
        UnsatisfiedSince_.reset();
    }
}

std::pair<TSchedulingSegmentMap<bool>, bool> TSchedulingSegmentManager::FindUnsatisfiedSegments(const TUpdateSchedulingSegmentsContext* context) const
{
    TSchedulingSegmentMap<bool> isSegmentUnsatisfied;
    bool hasUnsatisfiedSegment = false;
    for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
        const auto& fairResourceAmount = context->FairResourceAmountPerSegment.At(segment);
        const auto& currentResourceAmount = context->CurrentResourceAmountPerSegment.At(segment);
        if (IsModuleAwareSchedulingSegment(segment)) {
            for (const auto& schedulingSegmentModule : Config_->GetModules()) {
                if (currentResourceAmount.GetOrDefaultAt(schedulingSegmentModule) < fairResourceAmount.GetOrDefaultAt(schedulingSegmentModule)) {
                    hasUnsatisfiedSegment = true;
                    isSegmentUnsatisfied.At(segment).SetAt(schedulingSegmentModule, true);
                }
            }
        } else if (currentResourceAmount.GetOrDefault() < fairResourceAmount.GetOrDefault()) {
            hasUnsatisfiedSegment = true;
            isSegmentUnsatisfied.At(segment).Set(true);
        }
    }

    return {std::move(isSegmentUnsatisfied), hasUnsatisfiedSegment};
}

void TSchedulingSegmentManager::DoRebalanceSegments(TUpdateSchedulingSegmentsContext* context) const
{
    YT_LOG_DEBUG("Rebalancing node scheduling segments in tree");

    auto keyResource = GetSegmentBalancingKeyResource(Config_->Mode);

    TSchedulingSegmentMap<int> addedNodeCountPerSegment;
    TSchedulingSegmentMap<int> removedNodeCountPerSegment;
    TNodeMovePenalty totalPenalty;
    std::vector<std::pair<TNodeWithMovePenalty, ESchedulingSegment>> movedNodes;

    auto trySatisfySegment = [&] (
        ESchedulingSegment segment,
        double& currentResourceAmount,
        double fairResourceAmount,
        TNodeWithMovePenaltyList& availableNodes)
    {
        while (currentResourceAmount < fairResourceAmount) {
            if (availableNodes.empty()) {
                break;
            }

            // NB(eshcherbin): |availableNodes| is sorted in order of decreasing penalty.
            auto [nextAvailableNode, nextAvailableNodeMovePenalty] = availableNodes.back();
            availableNodes.pop_back();

            auto resourceAmountOnNode = GetNodeResourceLimit(*nextAvailableNode, keyResource);
            auto oldSegment = nextAvailableNode->SchedulingSegment;

            YT_LOG_DEBUG("Moving node to a new scheduling segment (Address: %v, OldSegment: %v, NewSegment: %v, Module: %v, Penalty: %v)",
                nextAvailableNode->Descriptor->Address,
                nextAvailableNode->SchedulingSegment,
                segment,
                GetNodeModule(*nextAvailableNode),
                nextAvailableNodeMovePenalty);

            SetNodeSegment(nextAvailableNode, segment, context);
            movedNodes.emplace_back(
                TNodeWithMovePenalty{.Node = nextAvailableNode, .MovePenalty = nextAvailableNodeMovePenalty},
                segment);
            totalPenalty += nextAvailableNodeMovePenalty;

            const auto& schedulingSegmentModule = GetNodeModule(*nextAvailableNode);
            currentResourceAmount += resourceAmountOnNode;
            if (IsModuleAwareSchedulingSegment(segment)) {
                ++addedNodeCountPerSegment.At(segment).MutableAt(schedulingSegmentModule);
            } else {
                ++addedNodeCountPerSegment.At(segment).Mutable();
            }

            if (IsModuleAwareSchedulingSegment(oldSegment)) {
                context->CurrentResourceAmountPerSegment.At(oldSegment).MutableAt(schedulingSegmentModule) -= resourceAmountOnNode;
                ++removedNodeCountPerSegment.At(oldSegment).MutableAt(schedulingSegmentModule);
            } else {
                context->CurrentResourceAmountPerSegment.At(oldSegment).Mutable() -= resourceAmountOnNode;
                ++removedNodeCountPerSegment.At(oldSegment).Mutable();
            }
        }
    };

    // Every node has a penalty for moving it to another segment. We collect a set of movable nodes
    // iteratively by taking the node with the lowest penalty until the remaining nodes can no longer
    // satisfy the fair resource amount determined by the strategy.
    // In addition, the rest of the nodes are called aggressively movable if the current segment is not module-aware.
    // The intuition is that we should be able to compensate for a loss of such a node from one module by moving
    // a node from another module to the segment.
    THashMap<TSchedulingSegmentModule, TNodeWithMovePenaltyList> movableNodesPerModule;
    THashMap<TSchedulingSegmentModule, TNodeWithMovePenaltyList> aggressivelyMovableNodesPerModule;
    GetMovableNodes(
        context,
        &movableNodesPerModule,
        &aggressivelyMovableNodesPerModule);

    // First, we try to satisfy all module-aware segments, one module at a time.
    // During this phase we are allowed to use the nodes from |aggressivelyMovableNodesPerModule|
    // if |movableNodesPerModule| is exhausted.
    for (const auto& schedulingSegmentModule : Config_->GetModules()) {
        for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
            if (!IsModuleAwareSchedulingSegment(segment)) {
                continue;
            }

            auto& currentResourceAmount = context->CurrentResourceAmountPerSegment.At(segment).MutableAt(schedulingSegmentModule);
            auto fairResourceAmount = context->FairResourceAmountPerSegment.At(segment).GetOrDefaultAt(schedulingSegmentModule);
            trySatisfySegment(segment, currentResourceAmount, fairResourceAmount, movableNodesPerModule[schedulingSegmentModule]);
            trySatisfySegment(segment, currentResourceAmount, fairResourceAmount, aggressivelyMovableNodesPerModule[schedulingSegmentModule]);
        }
    }

    TNodeWithMovePenaltyList movableNodes;
    for (const auto& [_, movableNodesAtModule] : movableNodesPerModule) {
        std::move(movableNodesAtModule.begin(), movableNodesAtModule.end(), std::back_inserter(movableNodes));
    }
    SortByPenalty(movableNodes);
    std::reverse(movableNodes.begin(), movableNodes.end());

    // Second, we try to satisfy all other segments using the remaining movable nodes.
    // Note that some segments might have become unsatisfied during the first phase
    // if we used any nodes from |aggressivelyMovableNodesPerModule|.
    for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
        if (IsModuleAwareSchedulingSegment(segment)) {
            continue;
        }

        auto& currentResourceAmount = context->CurrentResourceAmountPerSegment.At(segment).Mutable();
        auto fairResourceAmount = context->FairResourceAmountPerSegment.At(segment).GetOrDefault();
        trySatisfySegment(segment, currentResourceAmount, fairResourceAmount, movableNodes);
    }

    auto [isSegmentUnsatisfied, hasUnsatisfiedSegment] = FindUnsatisfiedSegments(context);
    YT_LOG_WARNING_IF(hasUnsatisfiedSegment,
        "Failed to satisfy all scheduling segments during rebalancing (IsSegmentUnsatisfied: %v)",
        isSegmentUnsatisfied);

    YT_LOG_DEBUG(
        "Finished node scheduling segments rebalancing "
        "(TotalMovedNodeCount: %v, AddedNodeCountPerSegment: %v, RemovedNodeCountPerSegment: %v, "
        "NewResourceAmountPerSegment: %v, TotalPenalty: %v)",
        movedNodes.size(),
        addedNodeCountPerSegment,
        removedNodeCountPerSegment,
        context->CurrentResourceAmountPerSegment,
        totalPenalty);
}

void TSchedulingSegmentManager::GetMovableNodes(
    TUpdateSchedulingSegmentsContext* context,
    THashMap<TSchedulingSegmentModule, TNodeWithMovePenaltyList>* movableNodesPerModule,
    THashMap<TSchedulingSegmentModule, TNodeWithMovePenaltyList>* aggressivelyMovableNodesPerModule) const
{
    auto keyResource = GetSegmentBalancingKeyResource(Config_->Mode);
    TSchedulingSegmentMap<std::vector<TNodeId>> nodeIdsPerSegment;
    for (const auto& [nodeId, node] : context->NodeStates) {
        auto& nodeIds = nodeIdsPerSegment.At(node.SchedulingSegment);
        if (IsModuleAwareSchedulingSegment(node.SchedulingSegment)) {
            nodeIds.MutableAt(GetNodeModule(node)).push_back(nodeId);
        } else {
            nodeIds.Mutable().push_back(nodeId);
        }
    }

    auto collectMovableNodes = [&] (double currentResourceAmount, double fairResourceAmount, const std::vector<TNodeId>& nodeIds) {
        TNodeWithMovePenaltyList segmentNodes;
        segmentNodes.reserve(nodeIds.size());
        for (auto nodeId : nodeIds) {
            auto& node = GetOrCrash(context->NodeStates, nodeId);
            segmentNodes.push_back(TNodeWithMovePenalty{
                .Node = &node,
                .MovePenalty = GetMovePenaltyForNode(node, Config_->Mode),
            });
        }

        SortByPenalty(segmentNodes);

        for (const auto& nodeWithMovePenalty : segmentNodes) {
            const auto* node = nodeWithMovePenalty.Node;
            if (node->SpecifiedSchedulingSegment) {
                continue;
            }

            const auto& schedulingSegmentModule = GetNodeModule(*node);
            auto resourceAmountOnNode = GetNodeResourceLimit(*node, keyResource);
            currentResourceAmount -= resourceAmountOnNode;
            if (currentResourceAmount >= fairResourceAmount) {
                (*movableNodesPerModule)[schedulingSegmentModule].push_back(nodeWithMovePenalty);
            } else if (!IsModuleAwareSchedulingSegment(node->SchedulingSegment)) {
                (*aggressivelyMovableNodesPerModule)[schedulingSegmentModule].push_back(nodeWithMovePenalty);
            }
        }
    };

    for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
        const auto& currentResourceAmount = context->CurrentResourceAmountPerSegment.At(segment);
        const auto& fairResourceAmount = context->FairResourceAmountPerSegment.At(segment);
        const auto& nodeIds = nodeIdsPerSegment.At(segment);
        if (IsModuleAwareSchedulingSegment(segment)) {
            for (const auto& schedulingSegmentModule : Config_->GetModules()) {
                collectMovableNodes(
                    currentResourceAmount.GetOrDefaultAt(schedulingSegmentModule),
                    fairResourceAmount.GetOrDefaultAt(schedulingSegmentModule),
                    nodeIds.GetOrDefaultAt(schedulingSegmentModule));
            }
        } else {
            collectMovableNodes(
                currentResourceAmount.GetOrDefault(),
                fairResourceAmount.GetOrDefault(),
                nodeIds.GetOrDefault());
        }
    }

    auto sortAndReverseMovableNodes = [&] (auto& movableNodes) {
        for (const auto& schedulingSegmentModule : Config_->GetModules()) {
            SortByPenalty(movableNodes[schedulingSegmentModule]);
            std::reverse(movableNodes[schedulingSegmentModule].begin(), movableNodes[schedulingSegmentModule].end());
        }
    };
    sortAndReverseMovableNodes(*movableNodesPerModule);
    sortAndReverseMovableNodes(*aggressivelyMovableNodesPerModule);

    if (Config_->EnableDetailedLogs) {
        auto collectMovableNodeIndices = [&] (const THashMap<TSchedulingSegmentModule, TNodeWithMovePenaltyList>& nodesPerModule) {
            THashMap<TNodeId, int> nodeIdToIndex;
            for (const auto& [module, movableNodes] : nodesPerModule) {
                for (int reverseIndex = 0; reverseIndex < std::ssize(movableNodes); ++reverseIndex) {
                    auto nodeId = movableNodes[reverseIndex].Node->Descriptor->Id;
                    int index = std::ssize(movableNodes) - 1 - reverseIndex;
                    nodeIdToIndex[nodeId] = index;
                }
            }

            return nodeIdToIndex;
        };

        auto movableNodeIdToIndex = collectMovableNodeIndices(*movableNodesPerModule);
        auto getNodeMovableIndex = [&] (TNodeId nodeId) {
            if (auto it = movableNodeIdToIndex.find(nodeId); it != movableNodeIdToIndex.end()) {
                return it->second;
            }
            return -1;
        };

        auto aggressivelyMovableNodeIdToIndex = collectMovableNodeIndices(*aggressivelyMovableNodesPerModule);
        auto getNodeAggressivelyMovableIndex = [&] (TNodeId nodeId) {
            if (auto it = aggressivelyMovableNodeIdToIndex.find(nodeId); it != aggressivelyMovableNodeIdToIndex.end()) {
                return it->second;
            }
            return -1;
        };

        // For better node log order.
        using TNodeLogOrderKey = std::tuple<ESchedulingSegment, TSchedulingSegmentModule, TNodeMovePenalty, int, int>;
        auto getNodeLogOrderKey = [&] (TNodeId nodeId) {
            const auto& node = GetOrCrash(context->NodeStates, nodeId);
            return TNodeLogOrderKey{
                node.SchedulingSegment,
                GetNodeModule(node),
                GetMovePenaltyForNode(node, Config_->Mode),
                getNodeMovableIndex(nodeId),
                getNodeAggressivelyMovableIndex(nodeId),
            };
        };

        auto sortedNodeIds = GetKeys(context->NodeStates);
        SortBy(sortedNodeIds, getNodeLogOrderKey);

        for (auto nodeId : sortedNodeIds) {
            const auto& node = GetOrCrash(context->NodeStates, nodeId);

            YT_LOG_DEBUG(
                "Considering node for scheduling segment rebalancing "
                "(NodeId: %v, Address: %v, Segment: %v, SpecifiedSegment: %v, Module: %v, "
                "MovePenalty: %v, RunningAllocationStatistics: %v, LastRunningAllocationStatisticsUpdateTime: %v, "
                "MovableNodeIndex: %v, AggressivelyMovableNodeIndex: %v)",
                nodeId,
                node.Descriptor->Address,
                node.SchedulingSegment,
                node.SpecifiedSchedulingSegment,
                GetNodeModule(node),
                GetMovePenaltyForNode(node, Config_->Mode),
                node.RunningAllocationStatistics,
                CpuInstantToInstant(node.LastRunningAllocationStatisticsUpdateTime.value_or(0)),
                getNodeMovableIndex(nodeId),
                getNodeAggressivelyMovableIndex(nodeId));
        }
    }
}

const TSchedulingSegmentModule& TSchedulingSegmentManager::GetNodeModule(const TFairShareTreeAllocationSchedulerNodeState& node) const
{
    YT_ASSERT(node.Descriptor);

    return GetNodeModule(node.Descriptor, Config_->ModuleType);
}

void TSchedulingSegmentManager::SetNodeSegment(
    TFairShareTreeAllocationSchedulerNodeState* node,
    ESchedulingSegment segment,
    TUpdateSchedulingSegmentsContext* context) const
{
    if (node->SchedulingSegment == segment) {
        return;
    }

    node->SchedulingSegment = segment;
    context->MovedNodes.push_back(TSetNodeSchedulingSegmentOptions{
        .NodeId = node->Descriptor->Id,
        .Segment = segment,
    });
}

void TSchedulingSegmentManager::LogAndProfileSegments(const TUpdateSchedulingSegmentsContext* context) const
{
    auto mode = Config_->Mode;
    bool segmentedSchedulingEnabled = mode != ESegmentedSchedulingMode::Disabled;
    if (segmentedSchedulingEnabled) {
        YT_LOG_DEBUG(
            "Scheduling segments state in tree "
            "(Mode: %v, Modules: %v, KeyResource: %v, FairSharePerSegment: %v, TotalKeyResourceLimit: %v, "
            "TotalCapacityPerModule: %v, FairResourceAmountPerSegment: %v, CurrentResourceAmountPerSegment: %v)",
            mode,
            Config_->GetModules(),
            GetSegmentBalancingKeyResource(mode),
            context->FairSharePerSegment,
            context->NodesTotalKeyResourceLimit,
            context->TotalCapacityPerModule,
            context->FairResourceAmountPerSegment,
            context->CurrentResourceAmountPerSegment);
    } else {
        YT_LOG_DEBUG("Segmented scheduling is disabled in tree, skipping");
    }

    TSensorBuffer sensorBuffer;
    if (segmentedSchedulingEnabled) {
        for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
            auto profileResourceAmountPerSegment = [&] (const TString& sensorName, const TSegmentToResourceAmount& resourceAmountMap) {
                const auto& valueAtSegment = resourceAmountMap.At(segment);
                if (IsModuleAwareSchedulingSegment(segment)) {
                    for (const auto& schedulingSegmentModule : Config_->GetModules()) {
                        TWithTagGuard guard(&sensorBuffer, "module", ToString(schedulingSegmentModule));
                        sensorBuffer.AddGauge(sensorName, valueAtSegment.GetOrDefaultAt(schedulingSegmentModule));
                    }
                } else {
                    sensorBuffer.AddGauge(sensorName, valueAtSegment.GetOrDefault());
                }
            };

            TWithTagGuard guard(&sensorBuffer, "segment", FormatEnum(segment));
            profileResourceAmountPerSegment("/fair_resource_amount", context->FairResourceAmountPerSegment);
            profileResourceAmountPerSegment("/current_resource_amount", context->CurrentResourceAmountPerSegment);
        }
    } else {
        for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
            TWithTagGuard guard(&sensorBuffer, "segment", FormatEnum(segment));
            if (IsModuleAwareSchedulingSegment(segment)) {
                guard.AddTag("module", ToString(NullModule));
            }

            sensorBuffer.AddGauge("/fair_resource_amount", 0.0);
            sensorBuffer.AddGauge("/current_resource_amount", 0.0);
        }
    }

    for (const auto& schedulingSegmentModule : Config_->GetModules()) {
        auto it = context->TotalCapacityPerModule.find(schedulingSegmentModule);
        auto moduleCapacity = it != context->TotalCapacityPerModule.end() ? it->second : 0.0;

        TWithTagGuard guard(&sensorBuffer, "module", ToString(schedulingSegmentModule));
        sensorBuffer.AddGauge("/module_capacity", moduleCapacity);
    }

    SensorProducer_->Update(std::move(sensorBuffer));
}

void TSchedulingSegmentManager::BuildPersistentState(TUpdateSchedulingSegmentsContext* context) const
{
    context->PersistentState = New<TPersistentSchedulingSegmentsState>();

    for (auto [nodeId, node] : context->NodeStates) {
        if (node.SchedulingSegment == ESchedulingSegment::Default) {
            continue;
        }

        EmplaceOrCrash(
            context->PersistentState->NodeStates,
            nodeId,
            TPersistentNodeSchedulingSegmentState{
                .Segment = node.SchedulingSegment,
                .Address = node.Descriptor->Address,
            });
    }

    for (const auto& [operationId, operationState] : context->OperationStates) {
        if (operationState->SchedulingSegmentModule) {
            EmplaceOrCrash(
                context->PersistentState->OperationStates,
                operationId,
                TPersistentOperationSchedulingSegmentState{
                    .Module = operationState->SchedulingSegmentModule,
                });
            }
        }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
