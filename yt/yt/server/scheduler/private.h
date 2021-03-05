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
DECLARE_REFCOUNTED_CLASS(TOperationElement)
DECLARE_REFCOUNTED_CLASS(TOperationElementSharedState)
DECLARE_REFCOUNTED_CLASS(TCompositeSchedulerElement)
DECLARE_REFCOUNTED_CLASS(TPool)
DECLARE_REFCOUNTED_CLASS(TRootElement)

DECLARE_REFCOUNTED_CLASS(TResourceTree)
DECLARE_REFCOUNTED_CLASS(TResourceTreeElement)
DECLARE_REFCOUNTED_STRUCT(IFairShareTreeHost)

DECLARE_REFCOUNTED_CLASS(TFairShareStrategyOperationController)
DECLARE_REFCOUNTED_STRUCT(IFairShareTreeSnapshot)
DECLARE_REFCOUNTED_CLASS(TFairShareTreeSnapshotImpl);
DECLARE_REFCOUNTED_CLASS(TFairShareTreeProfiler)

class TScheduleJobsContext;

class TJobMetrics;

using TJobCounter = THashMap<std::tuple<EJobType, EJobState>, std::pair<i64, NProfiling::TGauge>>;
using TAbortedJobCounter = THashMap<std::tuple<EJobType, EJobState, EAbortReason>, NProfiling::TCounter>;
using TCompletedJobCounter = THashMap<std::tuple<EJobType, EJobState, EInterruptReason>, NProfiling::TCounter>;

using TNonOwningOperationElementMap = THashMap<TOperationId, TOperationElement*>;
using TOperationElementMap = THashMap<TOperationId, TOperationElementPtr>;

using TNonOwningPoolMap = THashMap<TString, TPool*>;
using TPoolMap = THashMap<TString, TPoolPtr>;

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

inline const NLogging::TLogger SchedulerEventLogger{"SchedulerEventLog", /* essentital */ true};
inline const NLogging::TLogger SchedulerResourceMeteringLogger{"SchedulerResourceMetering", /* essentital */ true};

inline const NProfiling::TRegistry SchedulerProfiler{"/scheduler"};

static constexpr int MaxNodesWithoutPoolTreeToAlert = 10;

inline const TString EventLogPoolTreeKey{"tree_id"};
inline const TString ProfilingPoolTreeKey{"tree"};

inline const NLogging::TLogger StrategyLogger{"NodeShard"};
inline const NLogging::TLogger NodeShardLogger{"Strategy"};

inline constexpr char DefaultOperationTag[] = "default";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

