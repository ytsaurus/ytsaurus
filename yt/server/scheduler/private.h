#pragma once

#include "public.h"
#include "exec_node.h"
#include "job.h"
#include "operation.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSchedulerElement)
DECLARE_REFCOUNTED_CLASS(TSchedulerElementSharedState)
DECLARE_REFCOUNTED_CLASS(TOperationElement)
DECLARE_REFCOUNTED_CLASS(TOperationElementSharedState)
DECLARE_REFCOUNTED_CLASS(TFairShareTree)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyOperationController)
DECLARE_REFCOUNTED_CLASS(TCompositeSchedulerElement)
DECLARE_REFCOUNTED_CLASS(TPool)
DECLARE_REFCOUNTED_CLASS(TRootElement)

DECLARE_REFCOUNTED_STRUCT(IFairShareTreeSnapshot)

struct TFairShareContext;

class TJobMetrics;

using TOperationElementByIdMap = THashMap<TOperationId, TOperationElement*>;

using TJobCounter = TEnumIndexedVector<TEnumIndexedVector<i64, EJobType>, EJobState>;
using TAbortedJobCounter = TEnumIndexedVector<TJobCounter, EAbortReason>;
using TCompletedJobCounter = TEnumIndexedVector<TJobCounter, EInterruptReason>;

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

////////////////////////////////////////////////////////////////////////////////

extern const TString RootPoolName;
extern const TString DefaultTreeAttributeName;

extern const NLogging::TLogger SchedulerLogger;
extern const NProfiling::TProfiler SchedulerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

