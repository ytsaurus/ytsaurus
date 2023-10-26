#pragma once

#include "public.h"
#include "operation.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/client/scheduler/private.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IFairShareTree)
DECLARE_REFCOUNTED_STRUCT(IFairShareTreeElementHost)

struct IFairShareTreeHost;
struct IFairShareTreeJobSchedulerHost;

DECLARE_REFCOUNTED_CLASS(TSchedulerElement)
DECLARE_REFCOUNTED_CLASS(TSchedulerOperationElement)
DECLARE_REFCOUNTED_CLASS(TSchedulerCompositeElement)
DECLARE_REFCOUNTED_CLASS(TSchedulerPoolElement)
DECLARE_REFCOUNTED_CLASS(TSchedulerRootElement)

DECLARE_REFCOUNTED_CLASS(TResourceTree)
DECLARE_REFCOUNTED_CLASS(TResourceTreeElement)

DECLARE_REFCOUNTED_CLASS(TScheduleJobsContext)

DECLARE_REFCOUNTED_CLASS(TFairShareStrategyOperationController)
DECLARE_REFCOUNTED_CLASS(TFairShareTreeJobScheduler)
DECLARE_REFCOUNTED_CLASS(TFairShareTreeSnapshot)
DECLARE_REFCOUNTED_CLASS(TFairShareTreeSetSnapshot)
DECLARE_REFCOUNTED_CLASS(TFairShareTreeSchedulingSnapshot)
DECLARE_REFCOUNTED_CLASS(TFairShareTreeProfileManager)

class TJobMetrics;

DECLARE_REFCOUNTED_STRUCT(TDynamicAttributesListSnapshot)

////////////////////////////////////////////////////////////////////////////////

using TNonOwningElementList = std::vector<TSchedulerElement*>;

using TNonOwningOperationElementList = std::vector<TSchedulerOperationElement*>;
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

DEFINE_ENUM(EOperationSchedulingPriority,
    (High)
    (Medium)
);

DEFINE_ENUM(EOperationPreemptionPriority,
    (None)
    (Normal)
    (Aggressive)
    (SsdNormal)
    (SsdAggressive)
);

DEFINE_ENUM(EOperationPreemptionStatus,
    (AllowedUnconditionally)
    (AllowedConditionally)
    (ForbiddenSinceStarving)
    (ForbiddenSinceUnsatisfied)
    (ForbiddenInAncestorConfig)
);

DEFINE_ENUM(EJobPreemptionLevel,
    (SsdNonPreemptible)
    (SsdAggressivelyPreemptible)
    (NonPreemptible)
    (AggressivelyPreemptible)
    (Preemptible)
);

DEFINE_ENUM(EJobPreemptionStatus,
    (NonPreemptible)
    (AggressivelyPreemptible)
    (Preemptible)
);

DEFINE_ENUM(EJobSchedulingStage,
    (RegularHighPriority)
    (RegularMediumPriority)
    (RegularPackingFallback)

    (PreemptiveNormal)
    (PreemptiveAggressive)
    (PreemptiveSsdNormal)
    (PreemptiveSsdAggressive)
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

using TOperationElementsBySchedulingPriority = TEnumIndexedVector<EOperationSchedulingPriority, TNonOwningOperationElementList>;

using TOperationCountByPreemptionPriority = TEnumIndexedVector<EOperationPreemptionPriority, int>;
using TOperationPreemptionPriorityParameters = std::pair<EOperationPreemptionPriorityScope, /*ssdPriorityPreemptionEnabled*/ bool>;
using TOperationCountsByPreemptionPriorityParameters = THashMap<TOperationPreemptionPriorityParameters, TOperationCountByPreemptionPriority>;

using TPreemptionStatusStatisticsVector = TEnumIndexedVector<EOperationPreemptionStatus, int>;

using TJobPreemptionStatusMap = THashMap<TJobId, EJobPreemptionStatus>;
using TJobPreemptionStatusMapPerOperation = THashMap<TOperationId, TJobPreemptionStatusMap>;

DECLARE_REFCOUNTED_STRUCT(TRefCountedJobPreemptionStatusMapPerOperation)

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

constexpr int UndefinedSchedulingIndex = -1;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
