#include "profilers.h"
#include "private.h"

#include <yt/yt/library/ytprof/allocation_tag_profiler/allocation_tag_profiler.h>

namespace NYT::NRpcProxy {

using namespace NProfiling;
using namespace NYTProf;

////////////////////////////////////////////////////////////////////////////////

TRpcProxyHeapUsageProfiler::TRpcProxyHeapUsageProfiler(
    IInvokerPtr invoker,
    const THeapProfilerConfigPtr& config)
    : AllocationTagProfiler_(
        CreateAllocationTagProfiler(
            {
                RpcProxyRpcAllocationTag,
                RpcProxyUserAllocationTag,
            },
            std::move(invoker),
            config->SnapshotUpdatePeriod,
            config->SamplingRate))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
