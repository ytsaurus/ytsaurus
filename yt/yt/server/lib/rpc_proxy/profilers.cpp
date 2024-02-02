#include "profilers.h"
#include "private.h"

#include <yt/yt/server/lib/misc/profiling_helpers.h>

namespace NYT::NRpcProxy {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TRpcProxyHeapUsageProfiler::TRpcProxyHeapUsageProfiler(
    IInvokerPtr invoker,
    const THeapProfilerConfigPtr& config)
    : HeapProfiler_(
        CreateHeapProfilerWithTags(
            {
                RpcProxyRpcAllocationTag,
                RpcProxyUserAllocationTag
            },
            std::move(invoker),
            config->SnapshotUpdatePeriod,
            config->SamplingRate))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
