#include "pool_tree_snapshot_state.h"

namespace NYT::NScheduler::NStrategy::NPolicy {

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

TPoolTreeSnapshotState::TPoolTreeSnapshotState(
    TStaticAttributesList staticAttributesList,
    TOperationElementsBySchedulingPriority schedulableOperationsPerPriority,
    THashSet<int> ssdPriorityPreemptionMedia,
    TCachedAllocationPreemptionStatuses cachedAllocationPreemptionStatuses,
    std::vector<TSchedulingTagFilter> knownSchedulingTagFilters,
    TOperationCountsByPreemptionPriorityParameters operationCountsByPreemptionPriorityParameters,
    TOperationStateMap operationIdToState,
    TSharedOperationStateMap operationIdToSharedState)
    : StaticAttributesList_(std::move(staticAttributesList))
    , SchedulableOperationsPerPriority_(std::move(schedulableOperationsPerPriority))
    , SsdPriorityPreemptionMedia_(std::move(ssdPriorityPreemptionMedia))
    , CachedAllocationPreemptionStatuses_(std::move(cachedAllocationPreemptionStatuses))
    , KnownSchedulingTagFilters_(std::move(knownSchedulingTagFilters))
    , OperationCountsByPreemptionPriorityParameters_(std::move(operationCountsByPreemptionPriorityParameters))
    , OperationIdToState_(std::move(operationIdToState))
    , OperationIdToSharedState_(std::move(operationIdToSharedState))
{ }

const TOperationStatePtr& TPoolTreeSnapshotState::GetOperationState(const TPoolTreeOperationElement* element) const
{
    return GetOrCrash(OperationIdToState_, element->GetOperationId());
}

const TOperationSharedStatePtr& TPoolTreeSnapshotState::GetOperationSharedState(const TPoolTreeOperationElement* element) const
{
    return GetOrCrash(OperationIdToSharedState_, element->GetOperationId());
}

const TOperationStatePtr& TPoolTreeSnapshotState::GetEnabledOperationState(const TPoolTreeOperationElement* element) const
{
    const auto& operationState = StaticAttributesList_.AttributesOf(element).OperationState;
    YT_ASSERT(operationState);
    return operationState;
}

const TOperationSharedStatePtr& TPoolTreeSnapshotState::GetEnabledOperationSharedState(const TPoolTreeOperationElement* element) const
{
    const auto& operationSharedState = StaticAttributesList_.AttributesOf(element).OperationSharedState;
    YT_ASSERT(operationSharedState);
    return operationSharedState;
}

TDynamicAttributesListSnapshotPtr TPoolTreeSnapshotState::GetDynamicAttributesListSnapshot() const
{
    return DynamicAttributesListSnapshot_.Acquire();
}

void TPoolTreeSnapshotState::SetDynamicAttributesListSnapshot(const TDynamicAttributesListSnapshotPtr& dynamicAttributesListSnapshotPtr)
{
    DynamicAttributesListSnapshot_.Store(dynamicAttributesListSnapshotPtr);
}

    void TPoolTreeSnapshotState::ResetDynamicAttributesListSnapshot()
{
    DynamicAttributesListSnapshot_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
