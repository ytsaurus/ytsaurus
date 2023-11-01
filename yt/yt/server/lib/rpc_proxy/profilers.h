#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyHeapUsageProfiler
    : public TRefCounted
{
public:
    TRpcProxyHeapUsageProfiler(
        IInvokerPtr invoker,
        std::optional<TDuration> updatePeriod);

private:
    const THeapUsageProfilerPtr HeapProfiler_;
};

DEFINE_REFCOUNTED_TYPE(TRpcProxyHeapUsageProfiler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
