#pragma once

#include "public.h"

#include <yt/yt/library/ytprof/allocation_tag_profiler/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/program/config.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

class THttpProxyHeapUsageProfiler
    : public TRefCounted
{
public:
    THttpProxyHeapUsageProfiler(
        IInvokerPtr invoker,
        THeapProfilerConfigPtr config);

private:
    const NYTProf::THeapUsageProfilerPtr HeapProfiler_;
};

DEFINE_REFCOUNTED_TYPE(THttpProxyHeapUsageProfiler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
