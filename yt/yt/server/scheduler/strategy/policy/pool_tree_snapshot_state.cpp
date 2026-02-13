#include "pool_tree_snapshot_state.h"

#include <yt/yt/server/scheduler/strategy/pool_tree_snapshot.h>

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

} // namespace NYT::NScheduler::NStrategy::NPolicy
