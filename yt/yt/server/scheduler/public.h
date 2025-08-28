#pragma once

#include <yt/yt/server/scheduler/policy/public.h>

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/actions/callback.h>

#include <library/cpp/yt/string/format.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TOperation)
DECLARE_REFCOUNTED_CLASS(TAllocation)

using TAllocationList = std::list<TAllocationPtr>;

struct INodeManagerHost;

DECLARE_REFCOUNTED_CLASS(TNodeManager)
DECLARE_REFCOUNTED_CLASS(TNodeShard)
DECLARE_REFCOUNTED_CLASS(TExecNode)
DECLARE_REFCOUNTED_CLASS(TControllerAgent)

DECLARE_REFCOUNTED_CLASS(TOperationsCleaner)

DECLARE_REFCOUNTED_CLASS(TScheduler)
DECLARE_REFCOUNTED_CLASS(TControllerAgentTracker)

struct IEventLogHost;

DECLARE_REFCOUNTED_STRUCT(INodeHeartbeatStrategyProxy)

DECLARE_REFCOUNTED_STRUCT(IStrategy)
struct IStrategyHost;
DECLARE_REFCOUNTED_STRUCT(IOperationStrategyHost)

DECLARE_REFCOUNTED_STRUCT(ISchedulingHeartbeatContext)
DECLARE_REFCOUNTED_STRUCT(ISchedulingOperationController)
DECLARE_REFCOUNTED_STRUCT(IOperationController)

struct TOperationControllerInitializeResult;
struct TOperationControllerPrepareResult;
struct TOperationRevivalDescriptor;

class TMasterConnector;

DECLARE_REFCOUNTED_CLASS(TBootstrap)

DECLARE_REFCOUNTED_STRUCT(TPersistentStrategyState)
DECLARE_REFCOUNTED_STRUCT(TPersistentTreeState)
DECLARE_REFCOUNTED_STRUCT(TPersistentPoolState)

// TODO(mrkastep) Move to private.h
DECLARE_REFCOUNTED_CLASS(TStrategyOperationState)

DECLARE_REFCOUNTED_CLASS(TControllerRuntimeData)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAllocationPreemptionStatus,
    (NonPreemptible)
    (AggressivelyPreemptible)
    (Preemptible)
);

DEFINE_ENUM(EAllocationPreemptionReason,
    (Preemption)
    (AggressivePreemption)
    (SsdPreemption)
    (SsdAggressivePreemption)
    (GracefulPreemption)
    (ResourceOvercommit)
    (ResourceLimitsViolated)
    (IncompatibleSchedulingSegment)
    (FullHostAggressivePreemption)
    (EvictionFromSchedulingModule)
    (OperationBoundToOtherModule)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
