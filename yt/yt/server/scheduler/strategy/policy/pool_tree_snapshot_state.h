#pragma once

#include "public.h"
#include "private.h"
#include "structs.h"
#include "operation_shared_state.h"
#include "scheduling_policy.h"

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

constexpr int EmptySchedulingTagFilterIndex = -1;

////////////////////////////////////////////////////////////////////////////////

struct TStaticAttributes
{
    int SchedulingIndex = UndefinedSchedulingIndex;
    int SchedulingTagFilterIndex = EmptySchedulingTagFilterIndex;
    bool EffectiveAggressivePreemptionAllowed = true;
    bool EffectivePrioritySchedulingSegmentModuleAssignmentEnabled = false;
    // Used for checking if operation is hung.
    bool IsAliveAtUpdate = false;

    // Only for operations.
    EOperationSchedulingPriority SchedulingPriority = EOperationSchedulingPriority::Medium;
    TOperationStatePtr OperationState;
    TOperationSharedStatePtr OperationSharedState;
    bool AreRegularAllocationsOnSsdNodesAllowed = true;
};

////////////////////////////////////////////////////////////////////////////////

class TStaticAttributesList final
    : public std::vector<TStaticAttributes>
{
public:
    TStaticAttributes& AttributesOf(const TPoolTreeElement* element);
    const TStaticAttributes& AttributesOf(const TPoolTreeElement* element) const;
};

////////////////////////////////////////////////////////////////////////////////

class TPoolTreeSnapshotStateImpl
    : public TPoolTreeSnapshotState
{
public:
    DEFINE_BYREF_RO_PROPERTY(TStaticAttributesList, StaticAttributesList);
    DEFINE_BYREF_RO_PROPERTY(TOperationElementsBySchedulingPriority, SchedulableOperationsPerPriority);
    DEFINE_BYREF_RO_PROPERTY(THashSet<int>, SsdPriorityPreemptionMedia);
    DEFINE_BYREF_RO_PROPERTY(TCachedAllocationPreemptionStatuses, CachedAllocationPreemptionStatuses);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TSchedulingTagFilter>, KnownSchedulingTagFilters);
    DEFINE_BYREF_RO_PROPERTY(TOperationCountsByPreemptionPriorityParameters, OperationCountsByPreemptionPriorityParameters);

public:
    TPoolTreeSnapshotStateImpl(
        TStaticAttributesList staticAttributesList,
        TOperationElementsBySchedulingPriority schedulableOperationsPerPriority,
        THashSet<int> ssdPriorityPreemptionMedia,
        TCachedAllocationPreemptionStatuses cachedAllocationPreemptionStatuses,
        std::vector<TSchedulingTagFilter> knownSchedulingTagFilters,
        TOperationCountsByPreemptionPriorityParameters operationCountsByPreemptionPriorityParameters,
        TOperationStateMap operationIdToState,
        TSharedOperationStateMap operationIdToSharedState);

    const TOperationStatePtr& GetOperationState(const TPoolTreeOperationElement* element) const;
    const TOperationSharedStatePtr& GetOperationSharedState(const TPoolTreeOperationElement* element) const;

    //! Faster versions of |GetOperationState| and |GetOperationSharedState| which do not do an extra hashmap lookup and rely on tree indices instead.
    const TOperationStatePtr& GetEnabledOperationState(const TPoolTreeOperationElement* element) const;
    const TOperationSharedStatePtr& GetEnabledOperationSharedState(const TPoolTreeOperationElement* element) const;

    //! NB(eshcherbin): This is the only part of the snapshot, that isn't constant.
    // It's just much more convenient to store dynamic attributes list snapshot together with the tree snapshot.
    TDynamicAttributesListSnapshotPtr GetDynamicAttributesListSnapshot() const;

private:
    // NB(eshcherbin): Enabled operations' states are also stored in static attributes to eliminate a hashmap lookup during scheduling.
    TOperationStateMap OperationIdToState_;
    TSharedOperationStateMap OperationIdToSharedState_;
    TAtomicIntrusivePtr<TDynamicAttributesListSnapshot> DynamicAttributesListSnapshot_;

    void SetDynamicAttributesListSnapshot(const TDynamicAttributesListSnapshotPtr& dynamicAttributesListSnapshotPtr);
    void ResetDynamicAttributesListSnapshot();

    friend class TSchedulingPolicy;
};

DEFINE_REFCOUNTED_TYPE(TPoolTreeSnapshotStateImpl)

////////////////////////////////////////////////////////////////////////////////

TPoolTreeSnapshotStateImpl* GetPoolTreeSnapshotState(const TPoolTreeSnapshotPtr& treeSnapshot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
