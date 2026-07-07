#pragma once

#include <library/cpp/yt/misc/enum.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ISchedulingPolicyHost);
DECLARE_REFCOUNTED_STRUCT(ISchedulingPolicy);

DECLARE_REFCOUNTED_CLASS(TSchedulingPolicy)

DECLARE_REFCOUNTED_STRUCT(ISchedulingHeartbeatContext)

DECLARE_REFCOUNTED_STRUCT(TPersistentSchedulingSegmentsState)

DECLARE_REFCOUNTED_STRUCT(TPostUpdateContext)
DECLARE_REFCOUNTED_STRUCT(TPoolTreeSnapshotState)

DECLARE_REFCOUNTED_STRUCT(TPostUpdateContextImpl)
DECLARE_REFCOUNTED_CLASS(TPoolTreeSnapshotStateImpl)

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
    (PreemptiveDefaultGpuFullHost)
);

DEFINE_ENUM(EOperationPreemptionPriority,
    (None)
    (Normal)
    (Aggressive)
    (SsdNormal)
    (SsdAggressive)
    (DefaultGpuFullHost)
);

DEFINE_ENUM(EAllocationPreemptionReason,
    (Preemption)
    (AggressivePreemption)
    (SsdPreemption)
    (SsdAggressivePreemption)
    (DefaultGpuFullHostPreemption)
    (GracefulPreemption)
    (ResourceOvercommit)
    (ResourceLimitsViolated)
    (IncompatibleSchedulingSegment)
    (FullHostAggressivePreemption)
    (EvictionFromSchedulingModule)
    (OperationBoundToOtherModule)
    (NodeUnschedulable)
    (OperationUnregistered)
    (UnexpectedAllocation)
);

DEFINE_ENUM(EAllocationUpdateStatus,
    (Updated)
    (Disabled)
    (Unexpected)
    (Preempted)
);

////////////////////////////////////////////////////////////////////////////////

static inline constexpr int UndefinedSchedulingIndex = -1;

////////////////////////////////////////////////////////////////////////////////

static inline constexpr int FullHostGpuAllocationGpuDemand = 8;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
