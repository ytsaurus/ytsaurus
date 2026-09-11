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

//! Test-only view of the fitted balancing context.
struct TResourceContextSnapshot
{
    THashMap<TComputationId, THashMap<TResourceId, double>> ResourceConsumptionMultiplier;
    THashMap<TComputationId, double> TotalConsumptionMultiplier;
    THashMap<std::string, double> WorkerTotalCapacity;
};

TResourceContextSnapshot CollectResourceContextForTesting(
    const TFlowViewPtr& flowView,
    const TDynamicJobBalancerSpecPtr& balancerSpec,
    const TWorkerGroupId& workerGroup);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NBalancer
