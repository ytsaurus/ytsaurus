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
struct IFairShareTreeAllocationSchedulerHost;

DECLARE_REFCOUNTED_CLASS(TSchedulerElement)
DECLARE_REFCOUNTED_CLASS(TSchedulerOperationElement)
DECLARE_REFCOUNTED_CLASS(TSchedulerCompositeElement)
DECLARE_REFCOUNTED_CLASS(TSchedulerPoolElement)
DECLARE_REFCOUNTED_CLASS(TSchedulerRootElement)

DECLARE_REFCOUNTED_CLASS(TResourceTree)
DECLARE_REFCOUNTED_CLASS(TResourceTreeElement)

DECLARE_REFCOUNTED_CLASS(TScheduleAllocationsContext)

DECLARE_REFCOUNTED_CLASS(TFairShareStrategyOperationController)
DECLARE_REFCOUNTED_CLASS(TFairShareTreeAllocationScheduler)
DECLARE_REFCOUNTED_CLASS(TFairShareTreeSnapshot)
DECLARE_REFCOUNTED_CLASS(TFairShareTreeSetSnapshot)
DECLARE_REFCOUNTED_CLASS(TFairShareTreeSchedulingSnapshot)
DECLARE_REFCOUNTED_CLASS(TFairShareTreeProfileManager)

class TAllocationMetrics;

DECLARE_REFCOUNTED_STRUCT(TDynamicAttributesListSnapshot)

////////////////////////////////////////////////////////////////////////////////

using TNonOwningElementList = std::vector<TSchedulerElement*>;

using TNonOwningOperationElementList = std::vector<TSchedulerOperationElement*>;
using TNonOwningOperationElementMap = THashMap<TOperationId, TSchedulerOperationElement*>;
using TOperationElementMap = THashMap<TOperationId, TSchedulerOperationElementPtr>;
using TOperationIdToJobResources = THashMap<TOperationId, TJobResources>;

using TNonOwningPoolElementMap = THashMap<TString, TSchedulerPoolElement*>;
using TPoolElementMap = THashMap<TString, TSchedulerPoolElementPtr>;

using TNodeIdSet = THashSet<NNodeTrackerClient::TNodeId>;
using TNodeYsonList = std::vector<std::pair<NNodeTrackerClient::TNodeId, NYson::TYsonString>>;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESchedulableStatus,
    (Normal)
    (BelowFairShare)
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

DEFINE_ENUM(EAllocationPreemptionLevel,
    (SsdNonPreemptible)
    (SsdAggressivelyPreemptible)
    (NonPreemptible)
    (AggressivelyPreemptible)
    (Preemptible)
);

DEFINE_ENUM(EAllocationPreemptionStatus,
    (NonPreemptible)
    (AggressivelyPreemptible)
    (Preemptible)
);

DEFINE_ENUM(EAllocationSchedulingStage,
    (RegularHighPriority)
    (RegularMediumPriority)
    (RegularPackingFallback)

    (PreemptiveNormal)
    (PreemptiveAggressive)
    (PreemptiveSsdNormal)
    (PreemptiveSsdAggressive)
);

DEFINE_ENUM(EAllocationPreemptionReason,
    (Preemption)
    (AggressivePreemption)
    (SsdPreemption)
    (SsdAggressivePreemption)
    (GracefulPreemption)
    (ResourceOvercommit)
    (ResourceLimitsViolated)
);

DEFINE_ENUM(EGpuSchedulingLogEventType,
    (FairShareInfo)
    (OperationRegistered)
    (OperationUnregistered)
    (SchedulingSegmentsInfo)
    (OperationAssignedToModule)
    (FailedToAssignOperation)
    (OperationModuleAssignmentRevoked)
    (MovedNodes)
);

////////////////////////////////////////////////////////////////////////////////

using TOperationElementsBySchedulingPriority = TEnumIndexedArray<EOperationSchedulingPriority, TNonOwningOperationElementList>;

using TOperationCountByPreemptionPriority = TEnumIndexedArray<EOperationPreemptionPriority, int>;
using TOperationPreemptionPriorityParameters = std::pair<EOperationPreemptionPriorityScope, /*ssdPriorityPreemptionEnabled*/ bool>;
using TOperationCountsByPreemptionPriorityParameters = THashMap<TOperationPreemptionPriorityParameters, TOperationCountByPreemptionPriority>;

using TPreemptionStatusStatisticsVector = TEnumIndexedArray<EOperationPreemptionStatus, int>;

using TAllocationPreemptionStatusMap = THashMap<TAllocationId, EAllocationPreemptionStatus>;
using TAllocationPreemptionStatusMapPerOperation = THashMap<TOperationId, TAllocationPreemptionStatusMap>;

using TJobResourcesByTagFilter = THashMap<TSchedulingTagFilter, TJobResources>;

DECLARE_REFCOUNTED_STRUCT(TRefCountedAllocationPreemptionStatusMapPerOperation)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, SchedulerEventLogger, NLogging::TLogger("SchedulerEventLog").WithEssential());
YT_DEFINE_GLOBAL(const NLogging::TLogger, SchedulerStructuredLogger, NLogging::TLogger("SchedulerStructuredLog").WithEssential());
YT_DEFINE_GLOBAL(const NLogging::TLogger, SchedulerGpuEventLogger, NLogging::TLogger("SchedulerGpuStructuredLog").WithEssential());
YT_DEFINE_GLOBAL(const NLogging::TLogger, SchedulerResourceMeteringLogger, NLogging::TLogger("SchedulerResourceMetering").WithEssential());

YT_DEFINE_GLOBAL(const NProfiling::TProfiler, SchedulerProfiler, "/scheduler");

static constexpr int MaxNodesWithoutPoolTreeToAlert = 10;

inline const TString EventLogPoolTreeKey{"tree_id"};

inline const NLogging::TLogger StrategyLogger{"Strategy"};
inline const NLogging::TLogger NodeShardLogger{"NodeShard"};

inline constexpr char DefaultOperationTag[] = "default";

inline const TString InfinibandClusterNameKey{"infiniband_cluster_tag"};

constexpr int UndefinedSchedulingIndex = -1;

static constexpr int LargeGpuAllocationGpuDemand = 8;

static constexpr int CypressNodeLimit = 1'000'000;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
