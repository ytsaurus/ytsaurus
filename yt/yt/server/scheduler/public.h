#pragma once

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/actions/callback.h>

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

DECLARE_REFCOUNTED_STRUCT(ISchedulerStrategy)
struct ISchedulerStrategyHost;
struct IOperationStrategyHost;

DECLARE_REFCOUNTED_STRUCT(ISchedulingContext)
DECLARE_REFCOUNTED_STRUCT(IOperationControllerStrategyHost)
DECLARE_REFCOUNTED_STRUCT(IOperationController)

struct TOperationControllerInitializeResult;
struct TOperationControllerPrepareResult;
struct TOperationRevivalDescriptor;

class TMasterConnector;
class TBootstrap;

DECLARE_REFCOUNTED_CLASS(TPersistentStrategyState)
DECLARE_REFCOUNTED_CLASS(TPersistentTreeState)
DECLARE_REFCOUNTED_CLASS(TPersistentPoolState)
DECLARE_REFCOUNTED_CLASS(TPersistentSchedulingSegmentsState)

// TODO(mrkastep) Move to private.h
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyOperationState)

DECLARE_REFCOUNTED_CLASS(TControllerRuntimeData)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
