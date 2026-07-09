#pragma once

#include "public.h"

#include "job_balancer_result.h"

namespace NYT::NFlow::NBalancer {

////////////////////////////////////////////////////////////////////////////////

//! Synchronizes setting and getting data for a balancer.
class IBalanceAsyncSynchronizer
    : public TRefCounted
{
public:
    ~IBalanceAsyncSynchronizer() override = default;

    //! Start balancing loop if not started already.
    virtual void StartBalancing(const IInvokerPtr& invoker) = 0;
    //! Stop balancing loop if not stopped already.
    virtual void StopBalancing() = 0;
    //! Main balancing method with all conditions check.
    virtual TRebalanceResult DoBalance(
        const TFlowViewPtr& flowView,
        const THashMap<TComputationId, IComputationControllerPtr>& controllers,
        const TDynamicJobBalancerSpecPtr& balancerSpec,
        std::optional<TDuration> timeSinceSynced,
        EPipelineState targetState) = 0;
};

using IBalanceAsyncSynchronizerPtr = TIntrusivePtr<IBalanceAsyncSynchronizer>;

IBalanceAsyncSynchronizerPtr CreateBalanceAsyncSynchronizer(const NProfiling::TProfiler& profiler, const TWorkerGroupId& workerGroup);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NBalancer
