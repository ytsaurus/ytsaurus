#include "profilers.h"
#include "private.h"

#include <yt/yt/library/ytprof/allocation_tag_profiler/allocation_tag_profiler.h>

namespace NYT::NHttpProxy {

using namespace NProfiling;
using namespace NYTProf;

////////////////////////////////////////////////////////////////////////////////

THttpProxyHeapUsageProfiler::THttpProxyHeapUsageProfiler(
    IInvokerPtr invoker,
    THeapProfilerConfigPtr config)
    : HeapProfiler_(
        CreateHeapProfilerWithTags(
            {
                HttpProxyCommandAllocationTag,
                HttpProxyUserAllocationTag
            },
            std::move(invoker),
            std::move(config->SnapshotUpdatePeriod),
            config->SamplingRate))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
