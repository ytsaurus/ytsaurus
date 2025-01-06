#include "profilers.h"
#include "private.h"

#include <yt/yt/library/ytprof/allocation_tag_profiler/allocation_tag_profiler.h>

namespace NYT::NRpcProxy {

using namespace NProfiling;
using namespace NYTProf;

////////////////////////////////////////////////////////////////////////////////

TProxyHeapUsageProfiler::TProxyHeapUsageProfiler(
    IInvokerPtr invoker,
    const THeapProfilerConfigPtr& config)
    : AllocationTagProfiler_(
        CreateAllocationTagProfiler(
            {
                RpcProxyMethodAllocationTagKey,
                RpcProxyUserAllocationTagKey,
            },
            std::move(invoker),
            config->SnapshotUpdatePeriod,
            config->SamplingRate))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
