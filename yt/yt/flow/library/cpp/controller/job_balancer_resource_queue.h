#pragma once

#include "public.h"

#include "job_balancer_result.h"

namespace NYT::NFlow::NBalancer {

////////////////////////////////////////////////////////////////////////////////

//! |now| is used to compute the age of partitions (warmup); tests pass a simulated clock.
TRebalanceResult DoBalanceResourceQueue(
    const TFlowViewPtr& flowView,
    const TDynamicJobBalancerSpecPtr& balancerSpec,
    const TWorkerGroupId& workerGroup,
    TInstant now = TInstant::Now());

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
