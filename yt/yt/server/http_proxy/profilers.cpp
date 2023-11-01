#include "profilers.h"
#include "private.h"

#include <yt/yt/server/lib/misc/profiling_helpers.h>

namespace NYT::NHttpProxy {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

THttpProxyHeapUsageProfiler::THttpProxyHeapUsageProfiler(
    IInvokerPtr invoker,
    std::optional<TDuration> updatePeriod)
    : HeapProfiler_(
        CreateHeapProfilerWithTags(
            {
                HttpProxyCommandAllocationTag,
                HttpProxyUserAllocationTag
            },
            std::move(invoker),
            std::move(updatePeriod)))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
