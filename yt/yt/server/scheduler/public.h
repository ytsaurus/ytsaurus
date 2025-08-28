#pragma once

#include <yt/yt/server/scheduler/strategy/policy/public.h>

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/string/format.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TOperation)
DECLARE_REFCOUNTED_CLASS(TAllocation)

struct INodeManagerHost;

DECLARE_REFCOUNTED_CLASS(TNodeManager)
DECLARE_REFCOUNTED_CLASS(TNodeShard)
DECLARE_REFCOUNTED_CLASS(TExecNode)
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

// For each memory capacity gives the number of nodes with this much memory.
using TMemoryDistribution = THashMap<i64, int>;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, SchedulerEventLogger, NLogging::TLogger("SchedulerEventLog").WithEssential());
YT_DEFINE_GLOBAL(const NLogging::TLogger, SchedulerStructuredLogger, NLogging::TLogger("SchedulerStructuredLog").WithEssential());
YT_DEFINE_GLOBAL(const NLogging::TLogger, SchedulerGpuEventLogger, NLogging::TLogger("SchedulerGpuStructuredLog").WithEssential());
YT_DEFINE_GLOBAL(const NLogging::TLogger, SchedulerResourceMeteringLogger, NLogging::TLogger("SchedulerResourceMetering").WithEssential());

YT_DEFINE_GLOBAL(const NProfiling::TProfiler, SchedulerProfiler, "/scheduler");

////////////////////////////////////////////////////////////////////////////////

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
