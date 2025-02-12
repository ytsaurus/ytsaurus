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
    DEFINE_BYREF_RO_PROPERTY(TFairShareStrategyOperationControllerConfigPtr, ControllerConfig);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceUsage);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceLimits);
    DEFINE_BYREF_RO_PROPERTY(int, NodeCount);
    DEFINE_BYREF_RO_PROPERTY(TFairShareTreeSchedulingSnapshotPtr, SchedulingSnapshot);
    DEFINE_BYREF_RO_PROPERTY(TJobResourcesByTagFilter, ResourceLimitsByTagFilter);

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
        TFairShareTreeSchedulingSnapshotPtr schedulingSnapshot,
        TJobResourcesByTagFilter resourceLimitsByTagFilter);

    TSchedulerPoolElement* FindPool(const TString& poolName) const;
    TSchedulerOperationElement* FindEnabledOperationElement(TOperationId operationId) const;
    TSchedulerOperationElement* FindDisabledOperationElement(TOperationId operationId) const;

    bool IsElementEnabled(const TSchedulerElement* element) const;
};

DEFINE_REFCOUNTED_TYPE(TFairShareTreeSnapshot)

////////////////////////////////////////////////////////////////////////////////

class TFairShareTreeSetSnapshot
    : public TRefCounted
{
public:
    TFairShareTreeSetSnapshot(std::vector<IFairShareTreePtr> trees, int topologyVersion);

    THashMap<TString, IFairShareTreePtr> BuildIdToTreeMapping() const;

    DEFINE_BYREF_RO_PROPERTY(std::vector<IFairShareTreePtr>, Trees);
    DEFINE_BYVAL_RO_PROPERTY(int, TopologyVersion);
};

DEFINE_REFCOUNTED_TYPE(TFairShareTreeSetSnapshot)

////////////////////////////////////////////////////////////////////////////////

struct TResourceUsageSnapshot final
{
    NProfiling::TCpuInstant BuildTime;
    THashSet<TOperationId> AliveOperationIds;
    THashMap<TOperationId, TJobResources> OperationIdToResourceUsage;
    THashMap<TString, TJobResources> PoolToResourceUsage;
    // NB: these usages used in scheduler tree strategy at schedule allocation initialization stage;
    // it can be dropped from snapshot by the cost of less accurate values (without precommitted part).
    THashMap<TOperationId, TJobResources> OperationIdToResourceUsageWithPrecommit;
    THashMap<TString, TJobResources> PoolToResourceUsageWithPrecommit;
};

using TResourceUsageSnapshotPtr = TIntrusivePtr<TResourceUsageSnapshot>;

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Maybe move to helpers.cpp or somewhere else?
TResourceUsageSnapshotPtr BuildResourceUsageSnapshot(const TFairShareTreeSnapshotPtr& treeSnapshot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
