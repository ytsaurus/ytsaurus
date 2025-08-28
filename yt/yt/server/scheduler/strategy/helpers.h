#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/public.h>

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

TOperationPoolTreeRuntimeParametersPtr GetSchedulingOptionsPerPoolTree(const IOperationPtr& operation, const TString& treeId);

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerTreeAlertDescriptor
{
    ESchedulerAlertType Type;
    TString Message;
};

const std::vector<TSchedulerTreeAlertDescriptor>& GetSchedulerTreeAlertDescriptors();

bool IsSchedulerTreeAlertType(ESchedulerAlertType alertType);

////////////////////////////////////////////////////////////////////////////////

TJobResources ComputeAvailableResources(
    const TJobResources& resourceLimits,
    const TJobResources& resourceUsage,
    const TJobResources& resourceDiscount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
