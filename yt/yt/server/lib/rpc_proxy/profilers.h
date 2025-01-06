#pragma once

#include "public.h"

#include <yt/yt/library/ytprof/allocation_tag_profiler/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/program/config.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TProxyHeapUsageProfiler
    : public TRefCounted
{
public:
    TProxyHeapUsageProfiler(
        IInvokerPtr invoker,
        const THeapProfilerConfigPtr& config);

private:
    const NYTProf::IAllocationTagProfilerPtr AllocationTagProfiler_;
};

DEFINE_REFCOUNTED_TYPE(TProxyHeapUsageProfiler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
