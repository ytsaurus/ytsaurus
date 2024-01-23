#include "profilers.h"
#include "private.h"

#include <yt/yt/server/lib/misc/profiling_helpers.h>

namespace NYT::NHttpProxy {

using namespace NProfiling;

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
