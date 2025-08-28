#pragma once

#include <yt/yt/server/scheduler/strategy/policy/public.h>

#include <yt/yt/server/scheduler/common/public.h>

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/actions/callback.h>

#include <library/cpp/yt/string/format.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TOperation)

struct INodeManagerHost;
DECLARE_REFCOUNTED_CLASS(TNodeManager)
DECLARE_REFCOUNTED_CLASS(TNodeShard)

DECLARE_REFCOUNTED_CLASS(TControllerAgent)

DECLARE_REFCOUNTED_CLASS(TOperationsCleaner)

DECLARE_REFCOUNTED_CLASS(TScheduler)
DECLARE_REFCOUNTED_CLASS(TControllerAgentTracker)

struct IEventLogHost;
DECLARE_REFCOUNTED_STRUCT(IOperationController)

struct TOperationControllerInitializeResult;
struct TOperationControllerPrepareResult;
struct TOperationRevivalDescriptor;

class TMasterConnector;

DECLARE_REFCOUNTED_CLASS(TBootstrap)

DECLARE_REFCOUNTED_CLASS(TControllerRuntimeData)

////////////////////////////////////////////////////////////////////////////////

using TAllocationList = std::list<TAllocationPtr>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
