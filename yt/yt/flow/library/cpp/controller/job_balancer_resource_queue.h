#pragma once

#include "public.h"

#include "job_balancer_result.h"

namespace NYT::NFlow::NBalancer {

////////////////////////////////////////////////////////////////////////////////

TRebalanceResult DoBalanceResourceQueue(
    const TFlowViewPtr& flowView,
    const TDynamicJobBalancerSpecPtr& balancerSpec,
    const TWorkerGroupId& workerGroup);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NBalancer
