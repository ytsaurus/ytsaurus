#pragma once

#include <library/cpp/yt/misc/enum.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

struct ISchedulingPolicyHost;

DECLARE_REFCOUNTED_CLASS(TSchedulingPolicy)
DECLARE_REFCOUNTED_CLASS(TPoolTreeSnapshotState)

DECLARE_REFCOUNTED_STRUCT(TPersistentSchedulingSegmentsState)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAllocationPreemptionStatus,
    (NonPreemptible)
    (AggressivelyPreemptible)
    (Preemptible)
);

////////////////////////////////////////////////////////////////////////////////

static inline constexpr int UndefinedSchedulingIndex = -1;

////////////////////////////////////////////////////////////////////////////////

static inline constexpr int FullHostGpuAllocationGpuDemand = 8;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
