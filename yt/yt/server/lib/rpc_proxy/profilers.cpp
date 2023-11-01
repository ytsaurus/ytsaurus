#include "profilers.h"
#include "private.h"

#include <yt/yt/server/lib/misc/profiling_helpers.h>

namespace NYT::NRpcProxy {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TRpcProxyHeapUsageProfiler::TRpcProxyHeapUsageProfiler(
    IInvokerPtr invoker,
    std::optional<TDuration> updatePeriod)
    : HeapProfiler_(
        CreateHeapProfilerWithTags(
            {
                RpcProxyRpcAllocationTag,
                RpcProxyUserAllocationTag
            },
            std::move(invoker),
            std::move(updatePeriod)))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
