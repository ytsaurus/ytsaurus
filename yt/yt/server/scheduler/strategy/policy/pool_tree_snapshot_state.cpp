#include "pool_tree_snapshot_state.h"

#include "helpers.h"
#include "scheduling_segment_manager.h"

#include <yt/yt/server/scheduler/strategy/field_filter.h>
#include <yt/yt/server/scheduler/strategy/pool_tree_snapshot.h>

namespace NYT::NScheduler::NStrategy::NPolicy {

using NYTree::TFluentMap;

////////////////////////////////////////////////////////////////////////////////

TStaticAttributes& TStaticAttributesList::AttributesOf(const TPoolTreeElement* element)
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(*this));
    return (*this)[index];
}

const TStaticAttributes& TStaticAttributesList::AttributesOf(const TPoolTreeElement* element) const
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(*this));
    return (*this)[index];
}

////////////////////////////////////////////////////////////////////////////////

TPoolTreeSnapshotStateImpl::TPoolTreeSnapshotStateImpl(
    TStaticAttributesList staticAttributesList,
    TOperationElementsBySchedulingPriority schedulableOperationsPerPriority,
    THashSet<int> ssdPriorityPreemptionMedia,
    TCachedAllocationPreemptionStatuses cachedAllocationPreemptionStatuses,
    std::vector<TSchedulingTagFilter> knownSchedulingTagFilters,
    TOperationCountsByPreemptionPriorityParameters operationCountsByPreemptionPriorityParameters,
    TOperationStateMap operationIdToState,
    TSharedOperationStateMap operationIdToSharedState)
    : TPoolTreeSnapshotState(EPolicyKind::Classic)
    , StaticAttributesList_(std::move(staticAttributesList))
    , SchedulableOperationsPerPriority_(std::move(schedulableOperationsPerPriority))
    , SsdPriorityPreemptionMedia_(std::move(ssdPriorityPreemptionMedia))
    , CachedAllocationPreemptionStatuses_(std::move(cachedAllocationPreemptionStatuses))
    , KnownSchedulingTagFilters_(std::move(knownSchedulingTagFilters))
    , OperationCountsByPreemptionPriorityParameters_(std::move(operationCountsByPreemptionPriorityParameters))
    , OperationIdToState_(std::move(operationIdToState))
    , OperationIdToSharedState_(std::move(operationIdToSharedState))
{ }

const TOperationStatePtr& TPoolTreeSnapshotStateImpl::GetOperationState(const TPoolTreeOperationElement* element) const
{
    return GetOrCrash(OperationIdToState_, element->GetOperationId());
}

const TOperationSharedStatePtr& TPoolTreeSnapshotStateImpl::GetOperationSharedState(const TPoolTreeOperationElement* element) const
{
    return GetOrCrash(OperationIdToSharedState_, element->GetOperationId());
}

const TOperationStatePtr& TPoolTreeSnapshotStateImpl::GetEnabledOperationState(const TPoolTreeOperationElement* element) const
{
    const auto& operationState = StaticAttributesList_.AttributesOf(element).OperationState;
    YT_ASSERT(operationState);
    return operationState;
}

const TOperationSharedStatePtr& TPoolTreeSnapshotStateImpl::GetEnabledOperationSharedState(const TPoolTreeOperationElement* element) const
{
    const auto& operationSharedState = StaticAttributesList_.AttributesOf(element).OperationSharedState;
    YT_ASSERT(operationSharedState);
    return operationSharedState;
}

TDynamicAttributesListSnapshotPtr TPoolTreeSnapshotStateImpl::GetDynamicAttributesListSnapshot() const
{
    return DynamicAttributesListSnapshot_.Acquire();
}

void TPoolTreeSnapshotStateImpl::SetDynamicAttributesListSnapshot(const TDynamicAttributesListSnapshotPtr& dynamicAttributesListSnapshotPtr)
{
    DynamicAttributesListSnapshot_.Store(dynamicAttributesListSnapshotPtr);
}

    void TPoolTreeSnapshotStateImpl::ResetDynamicAttributesListSnapshot()
{
    DynamicAttributesListSnapshot_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

TPoolTreeSnapshotStateImpl* GetPoolTreeSnapshotState(const TPoolTreeSnapshotPtr& treeSnapshot)
{
    const auto& state = treeSnapshot->SchedulingPolicyState();
    YT_ASSERT(state);
    YT_ASSERT(state->PolicyKind == EPolicyKind::Classic);
    return static_cast<TPoolTreeSnapshotStateImpl*>(state.Get());
}

////////////////////////////////////////////////////////////////////////////////

TError TPoolTreeSnapshotStateImpl::CheckIsOperationStuck(
    const TPoolTreeSnapshot& treeSnapshot,
    const TPoolTreeOperationElement* element,
    TInstant now,
    TInstant activationTime,
    const TOperationStuckCheckOptionsPtr& options) const
{
    if (element->PersistentAttributes().StarvationStatus == EStarvationStatus::NonStarving) {
        return TError();
    }

    YT_VERIFY(treeSnapshot.IsElementEnabled(element));

    const auto& operationSharedState = GetEnabledOperationSharedState(element);
    {
        int deactivationCount = 0;
        auto deactivationReasonToCount = operationSharedState->GetDeactivationReasonsFromLastNonStarvingTime();
        for (auto reason : options->DeactivationReasons) {
            deactivationCount += deactivationReasonToCount[reason];
        }

        auto lastScheduleAllocationSuccessTime = operationSharedState->GetLastScheduleAllocationSuccessTime();
        if (activationTime + options->SafeTimeout < now &&
            lastScheduleAllocationSuccessTime + options->SafeTimeout < now &&
            element->GetStarvingSince().value_or(now) + options->SafeTimeout < now &&
            operationSharedState->GetRunningAllocationCount() == 0 &&
            deactivationCount > options->MinScheduleAllocationAttempts)
        {
            return TError("Operation has no successful scheduled allocations for a long period")
                << TErrorAttribute("period", options->SafeTimeout)
                << TErrorAttribute("deactivation_count", deactivationCount)
                << TErrorAttribute("last_schedule_allocation_success_time", lastScheduleAllocationSuccessTime)
                << TErrorAttribute("starving_since", element->GetStarvingSince());
        }
    }

    // NB(eshcherbin): See YT-14393.
    const auto& operationState = GetEnabledOperationState(element);
    {
        const auto& segment = operationState->SchedulingSegment;
        const auto& schedulingSegmentModule = operationState->SchedulingSegmentModule;
        if (segment && IsModuleAwareSchedulingSegment(*segment) && schedulingSegmentModule && !element->GetSchedulingTagFilter().IsEmpty()) {
            auto tagFilter = element->GetSchedulingTagFilter().GetBooleanFormula().GetFormula();
            bool isModuleFilter = false;
            for (const auto& possibleModule : treeSnapshot.TreeConfig()->SchedulingSegments->GetModules()) {
                auto moduleTag = TSchedulingSegmentManager::GetNodeTagFromModuleName(
                    possibleModule,
                    treeSnapshot.TreeConfig()->SchedulingSegments->ModuleType);
                // NB(eshcherbin): This doesn't cover all the cases, only the most usual.
                // Don't really want to check boolean formula satisfiability here.
                if (tagFilter == moduleTag) {
                    isModuleFilter = true;
                    break;
                }
            }

            auto operationModuleTag = TSchedulingSegmentManager::GetNodeTagFromModuleName(
                *schedulingSegmentModule,
                treeSnapshot.TreeConfig()->SchedulingSegments->ModuleType);
            if (isModuleFilter && tagFilter != operationModuleTag) {
                return TError(
                    "Operation has a module specified in the scheduling tag filter, which causes scheduling problems; "
                    "use \"scheduling_segment_modules\" spec option instead")
                    << TErrorAttribute("scheduling_tag_filter", tagFilter)
                    << TErrorAttribute("available_modules", treeSnapshot.TreeConfig()->SchedulingSegments->GetModules());
            }
        }
    }

    return TError();
}

void TPoolTreeSnapshotStateImpl::BuildOperationProgress(
    const TPoolTreeSnapshot& treeSnapshot,
    const TPoolTreeOperationElement* element,
    IStrategyHost* strategyHost,
    NYTree::TFluentMap fluent) const
{
    bool isEnabled = treeSnapshot.IsElementEnabled(element);
    const auto& operationState = isEnabled
        ? GetEnabledOperationState(element)
        : GetOperationState(element);
    const auto& operationSharedState = isEnabled
        ? GetEnabledOperationSharedState(element)
        : GetOperationSharedState(element);
    const auto& attributes = isEnabled
        ? StaticAttributesList_.AttributesOf(element)
        : TStaticAttributes{};
    auto minNeededResourcesWithDiskQuotaUnsatisfiedCount = operationSharedState->GetMinNeededResourcesWithDiskQuotaUnsatisfiedCount();

    fluent
        .Item("preemptible_job_count").Value(operationSharedState->GetPreemptibleAllocationCount())
        .Item("aggressively_preemptible_job_count").Value(operationSharedState->GetAggressivelyPreemptibleAllocationCount())
        .Item("scheduling_index").Value(attributes.SchedulingIndex)
        .Item("scheduling_priority").Value(attributes.SchedulingPriority)
        .Item("deactivation_reasons").Value(operationSharedState->GetDeactivationReasons())
        .Item("min_needed_resources_unsatisfied_count").Value(minNeededResourcesWithDiskQuotaUnsatisfiedCount)
        .Item("disk_quota_usage").BeginMap()
            .Do([&] (NYTree::TFluentMap fluent) {
                strategyHost->SerializeDiskQuota(operationSharedState->GetTotalDiskQuota(), fluent.GetConsumer());
            })
        .EndMap()
        .Item("are_regular_jobs_on_ssd_nodes_allowed").Value(attributes.AreRegularAllocationsOnSsdNodesAllowed)
        .Item("scheduling_segment").Value(operationState->SchedulingSegment)
        .Item("scheduling_segment_module").Value(operationState->SchedulingSegmentModule);
}

void TPoolTreeSnapshotStateImpl::BuildElementYson(
    const TPoolTreeSnapshot& treeSnapshot,
    const TPoolTreeElement* element,
    const TFieldFilter& filter,
    NYTree::TFluentMap fluent) const
{
    bool enabled = treeSnapshot.IsElementEnabled(element);
    const auto& attributes = enabled
        ? StaticAttributesList_.AttributesOf(element)
        : TStaticAttributes{};
    fluent
        .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "enabled", enabled)
        .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "aggressive_preemption_allowed", IsAggressivePreemptionAllowed(element))
        .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
            filter,
            "effective_aggressive_preemption_allowed",
            attributes.EffectiveAggressivePreemptionAllowed)
        .DoIf(!element->IsOperation(), [&] (NYTree::TFluentMap fluent) {
            fluent.ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
                filter,
                "priority_scheduling_segment_module_assignment_enabled",
                IsPrioritySchedulingSegmentModuleAssignmentEnabled(element));
        })
        .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
            filter,
            "effective_priority_scheduling_segment_module_assignment_enabled",
            attributes.EffectivePrioritySchedulingSegmentModuleAssignmentEnabled);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
