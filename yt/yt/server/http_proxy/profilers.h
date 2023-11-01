#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

class THttpProxyHeapUsageProfiler
    : public TRefCounted
{
public:
    THttpProxyHeapUsageProfiler(
        IInvokerPtr invoker,
        std::optional<TDuration> updatePeriod);

private:
    const THeapUsageProfilerPtr HeapProfiler_;
};

DEFINE_REFCOUNTED_TYPE(THttpProxyHeapUsageProfiler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
