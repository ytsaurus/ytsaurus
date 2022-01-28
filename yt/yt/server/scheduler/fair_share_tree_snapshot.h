#pragma once

#include "private.h"
#include "fair_share_tree_element.h"

#include <yt/yt/core/misc/atomic_ptr.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using TTreeSnapshotId = TGuid;

////////////////////////////////////////////////////////////////////////////////

class TFairShareTreeSnapshot
    : public TRefCounted
{
    DEFINE_BYVAL_RO_PROPERTY(TTreeSnapshotId, Id);

    DEFINE_BYREF_RO_PROPERTY(TSchedulerRootElementPtr, RootElement);
    DEFINE_BYREF_RO_PROPERTY(TNonOwningOperationElementMap, EnabledOperationMap);
    DEFINE_BYREF_RO_PROPERTY(TNonOwningOperationElementMap, DisabledOperationMap);
    DEFINE_BYREF_RO_PROPERTY(TNonOwningPoolElementMap, PoolMap);
    DEFINE_BYREF_RO_PROPERTY(TFairShareStrategyTreeConfigPtr, TreeConfig);
    DEFINE_BYREF_RO_PROPERTY(TFairShareStrategyOperationControllerConfigPtr, ControllerConfig)
    DEFINE_BYREF_RO_PROPERTY(TTreeSchedulingSegmentsState, SchedulingSegmentsState);
    DEFINE_BYREF_RO_PROPERTY(TCachedJobPreemptionStatuses, CachedJobPreemptionStatuses);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceUsage);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceLimits);
    DEFINE_BYREF_RO_PROPERTY(THashSet<int>, SsdPriorityPreemptionMedia);

public:
    TFairShareTreeSnapshot(
        TTreeSnapshotId id,
        TSchedulerRootElementPtr rootElement,
        TNonOwningOperationElementMap enabledOperationIdToElement,
        TNonOwningOperationElementMap disabledOperationIdToElement,
        TNonOwningPoolElementMap poolNameToElement,
        const TCachedJobPreemptionStatuses& cachedJobPreemptionStatuses,
        TFairShareStrategyTreeConfigPtr treeConfig,
        TFairShareStrategyOperationControllerConfigPtr controllerConfig,
        TTreeSchedulingSegmentsState schedulingSegmentsState,
        const TJobResources& resourceUsage,
        const TJobResources& resourceLimits,
        THashSet<int> ssdPriorityPreemptionMedia);

    TSchedulerPoolElement* FindPool(const TString& poolName) const;
    TSchedulerOperationElement* FindEnabledOperationElement(TOperationId operationId) const;
    TSchedulerOperationElement* FindDisabledOperationElement(TOperationId operationId) const;

    TDynamicAttributesListSnapshotPtr GetDynamicAttributesListSnapshot() const;
    void SetDynamicAttributesListSnapshot(TDynamicAttributesListSnapshotPtr value);

    void UpdateDynamicAttributesSnapshot(const TResourceUsageSnapshotPtr& resourceUsageSnapshot);

private:
    TAtomicPtr<TDynamicAttributesListSnapshot> DynamicAttributesListSnapshot_;
};

DEFINE_REFCOUNTED_TYPE(TFairShareTreeSnapshot)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
