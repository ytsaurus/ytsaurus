#pragma once

#include "public.h"
#include "exec_node.h"
#include "job.h"
#include "operation.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/profiler.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/client/scheduler/private.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSchedulerElement)
DECLARE_REFCOUNTED_CLASS(TSchedulerOperationElement)
DECLARE_REFCOUNTED_CLASS(TSchedulerOperationElementSharedState)
DECLARE_REFCOUNTED_CLASS(TSchedulerCompositeElement)
DECLARE_REFCOUNTED_CLASS(TSchedulerPoolElement)
DECLARE_REFCOUNTED_CLASS(TSchedulerRootElement)

DECLARE_REFCOUNTED_CLASS(TResourceTree)
DECLARE_REFCOUNTED_CLASS(TResourceTreeElement)
DECLARE_REFCOUNTED_STRUCT(IFairShareTreeHost)

DECLARE_REFCOUNTED_CLASS(TFairShareStrategyOperationController)
DECLARE_REFCOUNTED_STRUCT(IFairShareTreeSnapshot)
DECLARE_REFCOUNTED_CLASS(TFairShareTreeSnapshotImpl);
DECLARE_REFCOUNTED_CLASS(TFairShareTreeProfiler)

class TScheduleJobsContext;

class TJobMetrics;

////////////////////////////////////////////////////////////////////////////////

using TJobCounter = THashMap<std::tuple<EJobType, EJobState>, std::pair<i64, NProfiling::TGauge>>;
using TAbortedJobCounter = THashMap<std::tuple<EJobType, EJobState, EAbortReason>, NProfiling::TCounter>;
using TFailedJobCounter = THashMap<std::tuple<EJobType, EJobState>, NProfiling::TCounter>;
using TCompletedJobCounter = THashMap<std::tuple<EJobType, EJobState, EInterruptReason>, NProfiling::TCounter>;

using TNonOwningOperationElementMap = THashMap<TOperationId, TSchedulerOperationElement*>;
using TOperationElementMap = THashMap<TOperationId, TSchedulerOperationElementPtr>;

using TNonOwningPoolElementMap = THashMap<TString, TSchedulerPoolElement*>;
using TPoolElementMap = THashMap<TString, TSchedulerPoolElementPtr>;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESchedulableStatus,
    (Normal)
    (BelowFairShare)
);

DEFINE_ENUM(EJobRevivalPhase,
    (RevivingControllers)
    (ConfirmingJobs)
    (Finished)
);

DEFINE_ENUM(EResourceTreeIncreaseResult,
    (Success)
    (ElementIsNotAlive)
    (ResourceLimitExceeded)
);

DEFINE_ENUM(EResourceTreeElementKind,
    (Operation)
    (Pool)
    (Root)
);

DEFINE_ENUM(EJobPreemptionStatus,
    (NonPreemptable)
    (AggressivelyPreemptable)
    (Preemptable)
);

////////////////////////////////////////////////////////////////////////////////

using TJobPreemptionStatusMap = THashMap<TJobId, EJobPreemptionStatus>;
using TJobPreemptionStatusMapPerOperation = THashMap<TOperationId, TJobPreemptionStatusMap>;

DECLARE_REFCOUNTED_STRUCT(TRefCountedJobPreemptionStatusMapPerOperation);

////////////////////////////////////////////////////////////////////////////////

inline const auto SchedulerEventLogger = NLogging::TLogger("SchedulerEventLog").WithEssential();
inline const auto SchedulerResourceMeteringLogger = NLogging::TLogger("SchedulerResourceMetering").WithEssential();

inline const NProfiling::TProfiler SchedulerProfiler{"/scheduler"};

static constexpr int MaxNodesWithoutPoolTreeToAlert = 10;

inline const TString EventLogPoolTreeKey{"tree_id"};
inline const TString ProfilingPoolTreeKey{"tree"};

inline const NLogging::TLogger StrategyLogger{"NodeShard"};
inline const NLogging::TLogger NodeShardLogger{"Strategy"};

inline constexpr char DefaultOperationTag[] = "default";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

