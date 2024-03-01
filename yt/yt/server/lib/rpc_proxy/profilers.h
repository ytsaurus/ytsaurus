#pragma once

#include "public.h"

#include <yt/yt/library/ytprof/allocation_tag_profiler/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/program/config.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyHeapUsageProfiler
    : public TRefCounted
{
public:
    TRpcProxyHeapUsageProfiler(
        IInvokerPtr invoker,
        const THeapProfilerConfigPtr& config);

private:
    const NYTProf::THeapUsageProfilerPtr HeapProfiler_;
};

DEFINE_REFCOUNTED_TYPE(TRpcProxyHeapUsageProfiler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
