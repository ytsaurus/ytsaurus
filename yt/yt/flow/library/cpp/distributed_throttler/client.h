#pragma once

#include <yt/yt/flow/library/cpp/distributed_throttler/prefetching/client.h>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

NConcurrency::IThroughputThrottlerPtr CreateDistributedThrottler(
    TDistributedThrottlerClientConfigPtr config,
    std::function<NRpc::IChannelPtr()> channelProvider,
    std::function<TPriority()> priorityProvider,
    std::function<TQuotaClassId()> quotaClassProvider,
    IStatusProfilerPtr statusProfiler,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
