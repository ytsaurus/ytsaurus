#include "profilers.h"
#include "private.h"

#include <yt/yt/library/ytprof/allocation_tag_profiler/allocation_tag_profiler.h>

namespace NYT::NHttpProxy {

using namespace NProfiling;
using namespace NYTProf;

////////////////////////////////////////////////////////////////////////////////

TProxyHeapUsageProfiler::TProxyHeapUsageProfiler(
    IInvokerPtr invoker,
    THeapProfilerConfigPtr config)
    : AllocationTagProfiler_(
        CreateAllocationTagProfiler(
            {
                HttpProxyCommandAllocationTagKey,
                HttpProxyUserAllocationTagKey,
            },
            std::move(invoker),
            std::move(config->SnapshotUpdatePeriod),
            config->SamplingRate))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
