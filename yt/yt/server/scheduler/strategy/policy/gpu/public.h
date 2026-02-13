#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ISchedulingPolicy)

DECLARE_REFCOUNTED_CLASS(TSchedulingPolicy)
DECLARE_REFCOUNTED_CLASS(TDummySchedulingPolicy)

DECLARE_REFCOUNTED_STRUCT(TAssignment)

DECLARE_REFCOUNTED_CLASS(TOperation)
DECLARE_REFCOUNTED_CLASS(TNode)

DECLARE_REFCOUNTED_STRUCT(TGpuPlanUpdateStatistics)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
