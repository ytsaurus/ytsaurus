#pragma once

#include "public.h"
#include "operation.h"

#include <yt/yt/client/scheduler/private.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IPoolTree)
DECLARE_REFCOUNTED_STRUCT(IPoolTreeElementHost)

struct IPoolTreeHost;

DECLARE_REFCOUNTED_CLASS(TPoolTreeElement)
DECLARE_REFCOUNTED_CLASS(TPoolTreeOperationElement)
DECLARE_REFCOUNTED_CLASS(TPoolTreeCompositeElement)
DECLARE_REFCOUNTED_CLASS(TPoolTreePoolElement)
DECLARE_REFCOUNTED_CLASS(TPoolTreeRootElement)

DECLARE_REFCOUNTED_CLASS(TResourceTree)
DECLARE_REFCOUNTED_CLASS(TResourceTreeElement)

DECLARE_REFCOUNTED_CLASS(TStrategyOperationController)
DECLARE_REFCOUNTED_CLASS(TPoolTreeSnapshot)
DECLARE_REFCOUNTED_CLASS(TPoolTreeSetSnapshot)
DECLARE_REFCOUNTED_CLASS(TPoolTreeProfileManager)

////////////////////////////////////////////////////////////////////////////////

using TNonOwningElementList = std::vector<TPoolTreeElement*>;

using TNonOwningOperationElementList = std::vector<TPoolTreeOperationElement*>;
using TNonOwningOperationElementMap = THashMap<TOperationId, TPoolTreeOperationElement*>;
using TOperationElementMap = THashMap<TOperationId, TPoolTreeOperationElementPtr>;
using TOperationIdToJobResources = THashMap<TOperationId, TJobResources>;

using TNonOwningPoolElementMap = THashMap<TString, TPoolTreePoolElement*>;
using TPoolElementMap = THashMap<TString, TPoolTreePoolElementPtr>;

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

DEFINE_ENUM(EAllocationSchedulingStage,
    (RegularHighPriority)
    (RegularMediumPriority)
    (RegularPackingFallback)

    (PreemptiveNormal)
    (PreemptiveAggressive)
    (PreemptiveSsdNormal)
    (PreemptiveSsdAggressive)
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

DEFINE_ENUM(EUnutilizedResourceReason,
    (Unknown)
    (Timeout)
    (Throttling)
    (FinishedJobs)
    (NodeHasWaitingAllocations)
    (AcceptableFragmentation)
    (ExcessiveFragmentation)
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

inline const std::string EventLogPoolTreeKey{"tree_id"};

YT_DEFINE_GLOBAL(const NLogging::TLogger, StrategyLogger, "Strategy");
YT_DEFINE_GLOBAL(const NLogging::TLogger, NodeShardLogger, "NodeShard");

constexpr int UndefinedSchedulingIndex = -1;

static constexpr int FullHostGpuAllocationGpuDemand = 8;

static constexpr int CypressNodeLimit = 1'000'000;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
