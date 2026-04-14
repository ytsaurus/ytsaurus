#pragma once

#include "public.h"

#include <yt/yt/server/scheduler/strategy/policy/public.h>

#include <yt/yt/server/lib/scheduler/scheduling_tag.h>

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/misc/global.h>

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, StrategyLogger, "Strategy");

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TResourceTree)
DECLARE_REFCOUNTED_CLASS(TResourceTreeElement)

DECLARE_REFCOUNTED_STRUCT(TRefCountedAllocationPreemptionStatusMapPerOperation)

DECLARE_REFCOUNTED_CLASS(TStrategyOperationState)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESchedulableStatus,
    (Normal)
    (BelowFairShare)
);

DEFINE_ENUM(EResourceTreeIncreaseResult,
    (Success)
    (ElementIsNotAlive)
    (ResourceLimitExceeded)
    (AdditionalResourceLimitExceeded)
);

DEFINE_ENUM(EResourceTreeIncreasePreemptedResult,
    (Success)
    (NoResourceLimitsViolation)
    (ElementIsNotAlive)
);

DEFINE_ENUM(EResourceTreeElementKind,
    (Operation)
    (Pool)
    (Root)
);

DEFINE_ENUM(EOperationPreemptionStatus,
    (Allowed)
    (ForbiddenSinceStarving)
    (ForbiddenInAncestorConfig)
);

DEFINE_ENUM(EAllocationPreemptionLevel,
    (SsdNonPreemptible)
    (SsdAggressivelyPreemptible)
    (NonPreemptible)
    (AggressivelyPreemptible)
    (Preemptible)
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

using TPreemptionStatusStatisticsVector = TEnumIndexedArray<EOperationPreemptionStatus, int>;

using TAllocationPreemptionStatusMap = THashMap<TAllocationId, NPolicy::EAllocationPreemptionStatus>;
using TAllocationPreemptionStatusMapPerOperation = THashMap<TOperationId, TAllocationPreemptionStatusMap>;

using TJobResourcesByTagFilter = THashMap<TSchedulingTagFilter, TJobResources>;

using TNodeIdSet = THashSet<NNodeTrackerClient::TNodeId>;
using TNodeIdToAddress = THashMap<NNodeTrackerClient::TNodeId, std::string>;

////////////////////////////////////////////////////////////////////////////////

static constexpr int MaxNodesWithoutPoolTreeToAlert = 10;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
