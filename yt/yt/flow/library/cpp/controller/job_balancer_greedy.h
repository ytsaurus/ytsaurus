#pragma once

#include "public.h"

#include "job_balancer_result.h"

namespace NYT::NFlow::NBalancer {

////////////////////////////////////////////////////////////////////////////////

TRebalanceResult DoBalanceGreedy(
    const TFlowViewPtr& flowView,
    const THashMap<TComputationId, IComputationControllerPtr>& controllers,
    const TWorkerGroupId& workerGroup);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NBalancer
