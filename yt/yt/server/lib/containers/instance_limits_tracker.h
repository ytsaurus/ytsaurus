#pragma once

#include "public.h"

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

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
    void Stop();

    NYTree::IYPathServicePtr GetOrchidService();

private:
    void DoUpdateLimits();
    void DoBuildOrchid(NYson::IYsonConsumer* consumer) const;

    const IInstancePtr Instance_;
    const IInvokerPtr Invoker_;
    const NConcurrency::TPeriodicExecutorPtr Executor_;

    std::optional<double> CpuLimit_;
    std::optional<i64> MemoryLimit_;
    std::optional<i64> MemoryUsage_;
    bool Running_ = false;
};

DEFINE_REFCOUNTED_TYPE(TInstanceLimitsTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
