#pragma once

#include "public.h"
#include "exec_node.h"
#include "job.h"
#include "operation.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSchedulerElement)
DECLARE_REFCOUNTED_CLASS(TOperationElement)
DECLARE_REFCOUNTED_CLASS(TOperationElementSharedState)
DECLARE_REFCOUNTED_CLASS(TFairShareTree)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyOperationController)
DECLARE_REFCOUNTED_CLASS(TCompositeSchedulerElement)
DECLARE_REFCOUNTED_CLASS(TPool)
DECLARE_REFCOUNTED_CLASS(TRootElement)
DECLARE_REFCOUNTED_CLASS(TResourceTree)
DECLARE_REFCOUNTED_CLASS(TResourceTreeElement)
DECLARE_REFCOUNTED_CLASS(IPackingMetric)

DECLARE_REFCOUNTED_STRUCT(IFairShareTreeSnapshot)

class TFairShareContext;

class TJobMetrics;

using TRawOperationElementMap = THashMap<TOperationId, TOperationElement*>;
using TOperationElementMap = THashMap<TOperationId, TOperationElementPtr>;

using TRawPoolMap = THashMap<TString, TPool*>;
using TPoolMap = THashMap<TString, TPoolPtr>;

using TJobCounter = THashMap<std::tuple<EJobType, EJobState>, i64>;
using TAbortedJobCounter = THashMap<std::tuple<EJobType, EJobState, EAbortReason>, i64>;
using TCompletedJobCounter = THashMap<std::tuple<EJobType, EJobState, EInterruptReason>, i64>;

DEFINE_ENUM(ESchedulableStatus,
    (Normal)
    (BelowMinShare)
    (BelowFairShare)
);

DEFINE_ENUM(EJobRevivalPhase,
    (RevivingControllers)
    (ConfirmingJobs)
    (Finished)
);

extern const NLogging::TLogger SchedulerLogger;
extern const NProfiling::TProfiler SchedulerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

