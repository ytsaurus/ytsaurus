#pragma once

#include "public.h"
#include "operation.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/profiler.h>

#include <yt/yt/client/scheduler/private.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IFairShareTree)
DECLARE_REFCOUNTED_STRUCT(IFairShareTreeElementHost)

DECLARE_REFCOUNTED_CLASS(TSchedulerElement)
DECLARE_REFCOUNTED_CLASS(TSchedulerOperationElement)
DECLARE_REFCOUNTED_CLASS(TSchedulerOperationElementSharedState)
DECLARE_REFCOUNTED_CLASS(TSchedulerCompositeElement)
DECLARE_REFCOUNTED_CLASS(TSchedulerPoolElement)
DECLARE_REFCOUNTED_CLASS(TSchedulerRootElement)

DECLARE_REFCOUNTED_CLASS(TResourceTree)
DECLARE_REFCOUNTED_CLASS(TResourceTreeElement)

DECLARE_REFCOUNTED_CLASS(TFairShareStrategyOperationController)
DECLARE_REFCOUNTED_CLASS(TFairShareTreeJobScheduler)
DECLARE_REFCOUNTED_CLASS(TFairShareTreeSnapshot)
DECLARE_REFCOUNTED_CLASS(TFairShareTreeSchedulingSnapshot)
DECLARE_REFCOUNTED_CLASS(TFairShareTreeProfileManager)

class TScheduleJobsContext;

class TJobMetrics;

////////////////////////////////////////////////////////////////////////////////

// COMPAT(ignat)
using TJobCounter = THashMap<std::tuple<EJobType, EJobState>, std::pair<i64, NProfiling::TGauge>>;
using TAbortedJobCounter = THashMap<std::tuple<EJobType, EAbortReason, TString>, NProfiling::TCounter>;
using TFailedJobCounter = THashMap<std::tuple<EJobType, TString>, NProfiling::TCounter>;
using TCompletedJobCounter = THashMap<std::tuple<EJobType, EInterruptReason, TString>, NProfiling::TCounter>;
using TStartedJobCounter = THashMap<std::tuple<EJobType, TString>, NProfiling::TCounter>;

using TNonOwningOperationElementMap = THashMap<TOperationId, TSchedulerOperationElement*>;
using TOperationElementMap = THashMap<TOperationId, TSchedulerOperationElementPtr>;
using TOperationIdToJobResources = THashMap<TOperationId, TJobResources>;

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

DEFINE_ENUM(EOperationPreemptionPriority,
    (None)
    (Regular)
    (Aggressive)
    (SsdRegular)
    (SsdAggressive)
);

DEFINE_ENUM(EJobPreemptionLevel,
    (SsdNonPreemptable)
    (SsdAggressivelyPreemptable)
    (NonPreemptable)
    (AggressivelyPreemptable)
    (Preemptable)
);

DEFINE_ENUM(EJobPreemptionStatus,
    (NonPreemptable)
    (AggressivelyPreemptable)
    (Preemptable)
);

DEFINE_ENUM(EJobSchedulingStage,
    ((NonPreemptive)             (0))
    ((PackingFallback)           (1))
    ((Preemptive)                (100))
    ((AggressivelyPreemptive)    (101))
    ((SsdPreemptive)             (102))
    ((SsdAggressivelyPreemptive) (103))
);

DEFINE_ENUM(EJobPreemptionReason,
    (Preemption)
    (AggressivePreemption)
    (SsdPreemption)
    (SsdAggressivePreemption)
    (GracefulPreemption)
    (ResourceOvercommit)
    (ResourceLimitsViolated)
);

////////////////////////////////////////////////////////////////////////////////

using TJobPreemptionStatusMap = THashMap<TJobId, EJobPreemptionStatus>;
using TJobPreemptionStatusMapPerOperation = THashMap<TOperationId, TJobPreemptionStatusMap>;

DECLARE_REFCOUNTED_STRUCT(TRefCountedJobPreemptionStatusMapPerOperation);

////////////////////////////////////////////////////////////////////////////////

inline const auto SchedulerEventLogger = NLogging::TLogger("SchedulerEventLog").WithEssential();
inline const auto SchedulerStructuredLogger = NLogging::TLogger("SchedulerStructuredLog").WithEssential();
inline const auto SchedulerResourceMeteringLogger = NLogging::TLogger("SchedulerResourceMetering").WithEssential();

inline const NProfiling::TProfiler SchedulerProfiler{"/scheduler"};

static constexpr int MaxNodesWithoutPoolTreeToAlert = 10;

inline const TString EventLogPoolTreeKey{"tree_id"};

inline const NLogging::TLogger StrategyLogger{"Strategy"};
inline const NLogging::TLogger NodeShardLogger{"NodeShard"};

inline constexpr char DefaultOperationTag[] = "default";

inline const TString InfinibandClusterNameKey{"infiniband_cluster_tag"};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

