#pragma once

#include <library/cpp/yt/misc/enum.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

struct ISchedulingPolicyHost;
DECLARE_REFCOUNTED_CLASS(TSchedulingPolicy)

DECLARE_REFCOUNTED_STRUCT(ISchedulingHeartbeatContext)

DECLARE_REFCOUNTED_CLASS(TPoolTreeSnapshotState)

DECLARE_REFCOUNTED_STRUCT(TPersistentSchedulingSegmentsState)

////////////////////////////////////////////////////////////////////////////////

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

DEFINE_ENUM(EOperationPreemptionPriority,
    (None)
    (Normal)
    (Aggressive)
    (SsdNormal)
    (SsdAggressive)
);

DEFINE_ENUM(EAllocationPreemptionReason,
    (Preemption)
    (AggressivePreemption)
    (SsdPreemption)
    (SsdAggressivePreemption)
    (GracefulPreemption)
    (ResourceOvercommit)
    (ResourceLimitsViolated)
    (IncompatibleSchedulingSegment)
    (FullHostAggressivePreemption)
    (EvictionFromSchedulingModule)
    (OperationBoundToOtherModule)
);

////////////////////////////////////////////////////////////////////////////////

static inline constexpr int UndefinedSchedulingIndex = -1;

////////////////////////////////////////////////////////////////////////////////

static inline constexpr int FullHostGpuAllocationGpuDemand = 8;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
