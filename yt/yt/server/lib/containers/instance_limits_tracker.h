#pragma once

#include "public.h"

#include <yt/core/actions/signal.h>

#include <yt/core/concurrency/public.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

class TInstanceLimitsTracker
    : public TRefCounted
{
public:
    //! Raises when container limits change.
    DEFINE_SIGNAL(void(double, i64), LimitsUpdated);

public:
    TInstanceLimitsTracker(
        IInstancePtr instance,
        IInvokerPtr invoker,
        TDuration updatePeriod);

    void Start();

private:
    void DoUpdateLimits();

    const IInstancePtr Instance_;
    const IInvokerPtr Invoker_;
    const NConcurrency::TPeriodicExecutorPtr Executor_;

    std::optional<double> CpuLimit_;
    std::optional<i64> MemoryLimit_;
};

DEFINE_REFCOUNTED_TYPE(TInstanceLimitsTracker)

////////////////////////////////////////////////////////////////////////////////

TInstanceLimitsTrackerPtr CreateSelfPortoInstanceLimitsTracker(
    IPortoExecutorPtr executor,
    IInvokerPtr invoker,
    TDuration updatePeriod);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
