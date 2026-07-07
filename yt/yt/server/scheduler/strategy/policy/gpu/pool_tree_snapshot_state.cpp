#include "pool_tree_snapshot_state.h"

#include <yt/yt/server/scheduler/strategy/policy/scheduling_segment_manager.h>

#include <yt/yt/server/scheduler/strategy/field_filter.h>
#include <yt/yt/server/scheduler/strategy/pool_tree_element.h>
#include <yt/yt/server/scheduler/strategy/pool_tree_snapshot.h>

#include <yt/yt/server/lib/scheduler/config.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

using NYTree::TFluentMap;

////////////////////////////////////////////////////////////////////////////////

TPoolTreeSnapshotStateImpl::TPoolTreeSnapshotStateImpl(
    TOperationSnapshotStateMap operationStates,
    TNodeSnapshotStateMap nodeStates,
    TAllocationSnapshotStateMap allocationStates,
    TInstant snapshotTime)
    : TPoolTreeSnapshotState(EPolicyKind::Gpu)
    , OperationStates_(std::move(operationStates))
    , NodeStates_(std::move(nodeStates))
    , AllocationStates_(std::move(allocationStates))
    , SnapshotTime_(snapshotTime)
{ }

////////////////////////////////////////////////////////////////////////////////

TPoolTreeSnapshotStateImpl* GetPoolTreeSnapshotState(const TPoolTreeSnapshotPtr& treeSnapshot)
{
    const auto& state = treeSnapshot->SchedulingPolicyState();
    YT_ASSERT(state);
    YT_ASSERT(state->PolicyKind == EPolicyKind::Gpu);
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

    auto it = OperationStates_.find(element->GetOperationId());
    if (it == OperationStates_.end()) {
        return TError();
    }
    const auto& operationInfo = it->second;

    if (activationTime + options->SafeTimeout < now &&
        operationInfo.LastScheduleAllocationSuccessTime + options->SafeTimeout < now &&
        element->GetStarvingSince().value_or(now) + options->SafeTimeout < now &&
        operationInfo.RealizedAssignmentCount == 0)
    {
        return TError("Operation has no successful scheduled allocations for a long period")
            << TErrorAttribute("period", options->SafeTimeout)
            << TErrorAttribute("last_schedule_allocation_success_time", operationInfo.LastScheduleAllocationSuccessTime)
            << TErrorAttribute("starving_since", element->GetStarvingSince());
    }

    // Module-vs-tag-filter conflict check, ported from classic but sourcing
    // modules from the GPU tree-config (treeConfig->GpuSchedulingPolicy).
    const auto& tagFilter = element->GetSchedulingTagFilter();
    if (!tagFilter.IsEmpty() && operationInfo.SchedulingModule.has_value()) {
        const auto& gpuConfig = treeSnapshot.TreeConfig()->GpuSchedulingPolicy;
        YT_VERIFY(gpuConfig);

        auto tagFilterFormula = tagFilter.GetBooleanFormula().GetFormula();
        bool isModuleFilter = false;
        for (const auto& possibleModule : gpuConfig->Modules) {
            auto moduleTag = TSchedulingSegmentManager::GetNodeTagFromModuleName(
                possibleModule,
                gpuConfig->ModuleType);
            if (tagFilterFormula == moduleTag) {
                isModuleFilter = true;
                break;
            }
        }

        auto operationModuleTag = TSchedulingSegmentManager::GetNodeTagFromModuleName(
            *operationInfo.SchedulingModule,
            gpuConfig->ModuleType);
        if (isModuleFilter && tagFilterFormula != operationModuleTag) {
            return TError(
                "Operation has a module specified in the scheduling tag filter, which causes scheduling problems; "
                "use \"scheduling_segment_modules\" spec option instead")
                << TErrorAttribute("scheduling_tag_filter", tagFilterFormula)
                << TErrorAttribute("available_modules", gpuConfig->Modules);
        }
    }

    return TError();
}

void TPoolTreeSnapshotStateImpl::BuildOperationProgress(
    const TPoolTreeSnapshot& /*treeSnapshot*/,
    const TPoolTreeOperationElement* element,
    IStrategyHost* /*strategyHost*/,
    TFluentMap fluent) const
{
    auto it = OperationStates_.find(element->GetOperationId());
    if (it == OperationStates_.end()) {
        return;
    }
    const auto& info = it->second;

    fluent
        .Item("realized_assignment_count").Value(info.RealizedAssignmentCount)
        .Item("preliminary_assignment_count").Value(info.PreliminaryAssignmentCount)
        .Item("preemptible").Value(info.Preemptible)
        .Item("starving").Value(info.Starving);
}

void TPoolTreeSnapshotStateImpl::BuildElementYson(
    const TPoolTreeSnapshot& treeSnapshot,
    const TPoolTreeElement* element,
    const TFieldFilter& filter,
    NYTree::TFluentMap fluent) const
{
    bool enabled = treeSnapshot.IsElementEnabled(element);
    fluent
        .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "enabled", enabled);

    if (!element->IsOperation()) {
        return;
    }

    const auto* operationElement = static_cast<const TPoolTreeOperationElement*>(element);
    auto it = OperationStates_.find(operationElement->GetOperationId());
    if (it == OperationStates_.end()) {
        return;
    }
    const auto& info = it->second;

    fluent
        .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "scheduling_module", info.SchedulingModule);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
