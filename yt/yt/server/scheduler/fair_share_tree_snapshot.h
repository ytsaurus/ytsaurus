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
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceUsage);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceLimits);
    DEFINE_BYREF_RO_PROPERTY(int, NodeCount);
    DEFINE_BYREF_RO_PROPERTY(TFairShareTreeSchedulingSnapshotPtr, SchedulingSnapshot);

public:
    TFairShareTreeSnapshot(
        TTreeSnapshotId id,
        TSchedulerRootElementPtr rootElement,
        TNonOwningOperationElementMap enabledOperationIdToElement,
        TNonOwningOperationElementMap disabledOperationIdToElement,
        TNonOwningPoolElementMap poolNameToElement,
        TFairShareStrategyTreeConfigPtr treeConfig,
        TFairShareStrategyOperationControllerConfigPtr controllerConfig,
        const TJobResources& resourceUsage,
        const TJobResources& resourceLimits,
        int nodeCount,
        TFairShareTreeSchedulingSnapshotPtr schedulingSnapshot);

    TSchedulerPoolElement* FindPool(const TString& poolName) const;
    TSchedulerOperationElement* FindEnabledOperationElement(TOperationId operationId) const;
    TSchedulerOperationElement* FindDisabledOperationElement(TOperationId operationId) const;

    bool IsElementEnabled(const TSchedulerElement* element) const;
};

DEFINE_REFCOUNTED_TYPE(TFairShareTreeSnapshot)

////////////////////////////////////////////////////////////////////////////////

struct TResourceUsageSnapshot final
{
    NProfiling::TCpuInstant BuildTime;
    THashSet<TOperationId> AliveOperationIds;
    THashMap<TOperationId, TJobResources> OperationIdToResourceUsage;
    THashMap<TString, TJobResources> PoolToResourceUsage;
};

using TResourceUsageSnapshotPtr = TIntrusivePtr<TResourceUsageSnapshot>;

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Maybe move to helpers.cpp or somewhere else?
TResourceUsageSnapshotPtr BuildResourceUsageSnapshot(const TFairShareTreeSnapshotPtr& treeSnapshot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
