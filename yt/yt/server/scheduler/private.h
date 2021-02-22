#pragma once

#include "public.h"
#include "exec_node.h"
#include "job.h"
#include "operation.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/client/scheduler/private.h>

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

extern const NLogging::TLogger SchedulerEventLogger;
extern const NLogging::TLogger SchedulerResourceMeteringLogger;

extern const NProfiling::TRegistry SchedulerProfiler;

static constexpr int MaxNodesWithoutPoolTreeToAlert = 10;

extern const TString EventLogPoolTreeKey;
extern const TString ProfilingPoolTreeKey;

extern const NLogging::TLogger StrategyLogger;
extern const NLogging::TLogger NodeShardLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

