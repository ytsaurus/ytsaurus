#pragma once

#include <yt/yt/client/scheduler/public.h>

#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/misc/enum.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

//! Assignment ids are minted in the allocation id format: when an assignment is realized,
//! its id becomes the id of the resulting allocation.
using TAssignmentId = TAllocationId;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EGpuAssignmentPlanningStage,
    (FullHostModuleBound)
    (FullHostNonGang)
    (Normal)
    (WithExtraResources)
    (LimitsCheck)
);

DEFINE_ENUM(EGpuSchedulingLogEventType,
    (OperationRegistered)
    (OperationUnregistered)
    (AssignmentAdded)
    (AssignmentPreempted)
    (AllocationScheduled)
    (AllocationPreempted)
    (OperationBoundToModule)
    (ModulesInfo)
    (NodesInfo)
    (OperationsInfo)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSchedulingPolicy)

DECLARE_REFCOUNTED_STRUCT(TAssignment)

DECLARE_REFCOUNTED_CLASS(TAllocationState)

DECLARE_REFCOUNTED_CLASS(TOperation)
DECLARE_REFCOUNTED_CLASS(TNode)

DECLARE_REFCOUNTED_STRUCT(TGpuPlanUpdateStatistics)

struct IAssignmentPlanUpdateContext;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
