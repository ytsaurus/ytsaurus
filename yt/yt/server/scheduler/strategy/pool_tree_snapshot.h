#pragma once

#include "private.h"
#include "pool_tree_element.h"

#include <yt/yt/server/scheduler/strategy/policy/public.h>

#include <yt/yt/core/misc/atomic_ptr.h>

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

using TTreeSnapshotId = TGuid;

////////////////////////////////////////////////////////////////////////////////

class TPoolTreeSnapshot
    : public TRefCounted
{
    DEFINE_BYVAL_RO_PROPERTY(TTreeSnapshotId, Id);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, Now);

    DEFINE_BYREF_RO_PROPERTY(TPoolTreeRootElementPtr, RootElement);
    DEFINE_BYREF_RO_PROPERTY(TNonOwningOperationElementMap, EnabledOperationMap);
    DEFINE_BYREF_RO_PROPERTY(TNonOwningOperationElementMap, DisabledOperationMap);
    DEFINE_BYREF_RO_PROPERTY(TNonOwningPoolElementMap, PoolMap);
    DEFINE_BYREF_RO_PROPERTY(TStrategyTreeConfigPtr, TreeConfig);
    DEFINE_BYREF_RO_PROPERTY(TStrategyOperationControllerConfigPtr, ControllerConfig);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceUsage);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceLimits);
    DEFINE_BYREF_RO_PROPERTY(TNodeIdToAddress, NodeAddresses);
    DEFINE_BYREF_RO_PROPERTY(NPolicy::TPoolTreeSnapshotStatePtr, SchedulingPolicyState);
    DEFINE_BYREF_RO_PROPERTY(TJobResourcesByTagFilter, ResourceLimitsByTagFilter);

public:
    TPoolTreeSnapshot(
        TTreeSnapshotId id,
        TInstant now,
        TPoolTreeRootElementPtr rootElement,
        TNonOwningOperationElementMap enabledOperationIdToElement,
        TNonOwningOperationElementMap disabledOperationIdToElement,
        TNonOwningPoolElementMap poolNameToElement,
        TStrategyTreeConfigPtr treeConfig,
        TStrategyOperationControllerConfigPtr controllerConfig,
        const TJobResources& resourceUsage,
        const TJobResources& resourceLimits,
        TNodeIdToAddress nodeAddresses,
        NPolicy::TPoolTreeSnapshotStatePtr schedulingPolicyState,
        TJobResourcesByTagFilter resourceLimitsByTagFilter);

    TPoolTreePoolElement* FindPool(const TString& poolName) const;
    TPoolTreeOperationElement* FindEnabledOperationElement(TOperationId operationId) const;
    TPoolTreeOperationElement* FindDisabledOperationElement(TOperationId operationId) const;

    bool IsElementEnabled(const TPoolTreeElement* element) const;
};

DEFINE_REFCOUNTED_TYPE(TPoolTreeSnapshot)

////////////////////////////////////////////////////////////////////////////////

class TPoolTreeSetSnapshot
    : public TRefCounted
{
public:
    TPoolTreeSetSnapshot(std::vector<IPoolTreePtr> trees, int topologyVersion);

    THashMap<std::string, IPoolTreePtr> BuildIdToTreeMapping() const;

    DEFINE_BYREF_RO_PROPERTY(std::vector<IPoolTreePtr>, Trees);
    DEFINE_BYVAL_RO_PROPERTY(int, TopologyVersion);
};

DEFINE_REFCOUNTED_TYPE(TPoolTreeSetSnapshot)

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

DEFINE_REFCOUNTED_TYPE(TResourceUsageSnapshot)

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Maybe move to helpers.cpp or somewhere else?
TResourceUsageSnapshotPtr BuildResourceUsageSnapshot(const TPoolTreeSnapshotPtr& treeSnapshot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
