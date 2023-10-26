#include "fair_share_tree_scheduling_snapshot.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TStaticAttributes& TStaticAttributesList::AttributesOf(const TSchedulerElement* element)
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(*this));
    return (*this)[index];
}

const TStaticAttributes& TStaticAttributesList::AttributesOf(const TSchedulerElement* element) const
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(*this));
    return (*this)[index];
}

////////////////////////////////////////////////////////////////////////////////

TFairShareTreeSchedulingSnapshot::TFairShareTreeSchedulingSnapshot(
    TStaticAttributesList staticAttributesList,
    TOperationElementsBySchedulingPriority schedulableOperationsPerPriority,
    THashSet<int> ssdPriorityPreemptionMedia,
    TCachedJobPreemptionStatuses cachedJobPreemptionStatuses,
    std::vector<TSchedulingTagFilter> knownSchedulingTagFilters,
    TOperationCountsByPreemptionPriorityParameters operationCountsByPreemptionPriorityParameters,
    TFairShareTreeJobSchedulerOperationStateMap operationIdToState,
    TFairShareTreeJobSchedulerSharedOperationStateMap operationIdToSharedState)
    : StaticAttributesList_(std::move(staticAttributesList))
    , SchedulableOperationsPerPriority_(std::move(schedulableOperationsPerPriority))
    , SsdPriorityPreemptionMedia_(std::move(ssdPriorityPreemptionMedia))
    , CachedJobPreemptionStatuses_(std::move(cachedJobPreemptionStatuses))
    , KnownSchedulingTagFilters_(std::move(knownSchedulingTagFilters))
    , OperationCountsByPreemptionPriorityParameters_(std::move(operationCountsByPreemptionPriorityParameters))
    , OperationIdToState_(std::move(operationIdToState))
    , OperationIdToSharedState_(std::move(operationIdToSharedState))
{ }

const TFairShareTreeJobSchedulerOperationStatePtr& TFairShareTreeSchedulingSnapshot::GetOperationState(const TSchedulerOperationElement* element) const
{
    return GetOrCrash(OperationIdToState_, element->GetOperationId());
}

const TFairShareTreeJobSchedulerOperationSharedStatePtr& TFairShareTreeSchedulingSnapshot::GetOperationSharedState(const TSchedulerOperationElement* element) const
{
    return GetOrCrash(OperationIdToSharedState_, element->GetOperationId());
}

const TFairShareTreeJobSchedulerOperationStatePtr& TFairShareTreeSchedulingSnapshot::GetEnabledOperationState(const TSchedulerOperationElement* element) const
{
    const auto& operationState = StaticAttributesList_.AttributesOf(element).OperationState;
    YT_ASSERT(operationState);
    return operationState;
}

const TFairShareTreeJobSchedulerOperationSharedStatePtr& TFairShareTreeSchedulingSnapshot::GetEnabledOperationSharedState(const TSchedulerOperationElement* element) const
{
    const auto& operationSharedState = StaticAttributesList_.AttributesOf(element).OperationSharedState;
    YT_ASSERT(operationSharedState);
    return operationSharedState;
}

TDynamicAttributesListSnapshotPtr TFairShareTreeSchedulingSnapshot::GetDynamicAttributesListSnapshot() const
{
    return DynamicAttributesListSnapshot_.Acquire();
}

void TFairShareTreeSchedulingSnapshot::SetDynamicAttributesListSnapshot(const TDynamicAttributesListSnapshotPtr& dynamicAttributesListSnapshotPtr)
{
    DynamicAttributesListSnapshot_.Store(dynamicAttributesListSnapshotPtr);
}

    void TFairShareTreeSchedulingSnapshot::ResetDynamicAttributesListSnapshot()
{
    DynamicAttributesListSnapshot_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
