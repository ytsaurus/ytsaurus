#pragma once

#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/misc/enum.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EGpuAssignmentPlanningStage,
    (FullHostModuleBound)
    (Normal)
    (WithExtraResources)
    (LimitsCheck)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSchedulingPolicy)

DECLARE_REFCOUNTED_STRUCT(TAssignment)

DECLARE_REFCOUNTED_CLASS(TAllocationState)

DECLARE_REFCOUNTED_CLASS(TOperation)
DECLARE_REFCOUNTED_CLASS(TNode)

DECLARE_REFCOUNTED_STRUCT(TGpuPlanUpdateStatistics)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
