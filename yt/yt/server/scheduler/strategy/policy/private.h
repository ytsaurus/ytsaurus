#pragma once

#include "public.h"

#include <yt/yt/server/scheduler/public.h>

#include <yt/yt/server/lib/scheduler/structs.h>

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TGpuSchedulerAssignment)

DECLARE_REFCOUNTED_CLASS(TGpuSchedulerOperation)
DECLARE_REFCOUNTED_CLASS(TGpuSchedulerNode)

DECLARE_REFCOUNTED_CLASS(TScheduleAllocationsContext)

DECLARE_REFCOUNTED_STRUCT(TDynamicAttributesListSnapshot)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
