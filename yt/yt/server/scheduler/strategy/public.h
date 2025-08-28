#pragma once

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/client/scheduler/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IStrategy)
struct IStrategyHost;
DECLARE_REFCOUNTED_STRUCT(IOperation)

DECLARE_REFCOUNTED_STRUCT(INodeHeartbeatStrategyProxy)

DECLARE_REFCOUNTED_STRUCT(ISchedulingHeartbeatContext)
DECLARE_REFCOUNTED_STRUCT(ISchedulingOperationController)

DECLARE_REFCOUNTED_STRUCT(IPoolTree)
DECLARE_REFCOUNTED_STRUCT(IPoolTreeElementHost)

struct IPoolTreeHost;

DECLARE_REFCOUNTED_CLASS(TPoolTreeElement)
DECLARE_REFCOUNTED_CLASS(TPoolTreeOperationElement)
DECLARE_REFCOUNTED_CLASS(TPoolTreeCompositeElement)
DECLARE_REFCOUNTED_CLASS(TPoolTreePoolElement)
DECLARE_REFCOUNTED_CLASS(TPoolTreeRootElement)

DECLARE_REFCOUNTED_CLASS(TOperationController)
DECLARE_REFCOUNTED_CLASS(TPoolTreeSnapshot)
DECLARE_REFCOUNTED_CLASS(TPoolTreeSetSnapshot)
DECLARE_REFCOUNTED_CLASS(TPoolTreeProfileManager)

DECLARE_REFCOUNTED_STRUCT(TPersistentStrategyState)
DECLARE_REFCOUNTED_STRUCT(TPersistentTreeState)
DECLARE_REFCOUNTED_STRUCT(TPersistentPoolState)

////////////////////////////////////////////////////////////////////////////////

using TNonOwningElementList = std::vector<TPoolTreeElement*>;

using TNonOwningOperationElementList = std::vector<TPoolTreeOperationElement*>;
using TNonOwningOperationElementMap = THashMap<TOperationId, TPoolTreeOperationElement*>;
using TOperationElementMap = THashMap<TOperationId, TPoolTreeOperationElementPtr>;
using TOperationIdToJobResources = THashMap<TOperationId, TJobResources>;

using TNonOwningPoolElementMap = THashMap<TString, TPoolTreePoolElement*>;
using TPoolElementMap = THashMap<TString, TPoolTreePoolElementPtr>;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUnschedulableReason,
    (IsNotRunning)
    (Suspended)
    (NoPendingAllocations)

    // NB(eshcherbin): This is not exactly an "unschedulable" reason, but it is
    // reasonable in our architecture to put it here anyway.
    (MaxScheduleAllocationCallsViolated)
    (FifoSchedulableElementCountLimitReached)
);

////////////////////////////////////////////////////////////////////////////////

static inline const std::string EventLogPoolTreeKey{"tree_id"};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
