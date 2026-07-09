#pragma once

#include "config.h"
#include "public.h"

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

//! Creates a distributed throttler client (IThroughputThrottlerPtr).
//! Internally: TPrefetchingThrottler wraps TRemoteThrottler, which wraps
//! |channelProvider|'s result in a retrying channel on every RPC.
//!
//! |channelProvider| is called per RPC, so a new controller leader is
//! picked up as soon as the provider returns a channel to it; null means
//! the call fails and TPrefetchingThrottler retries later.
//!
//! |priorityProvider| returns the priority key for each RequestQuota
//! (smaller == higher priority on the server queue).
//!
//! |statusProfiler| is optional and receives RPC-health reports.
NConcurrency::IThroughputThrottlerPtr CreateDistributedThrottler(
    TDistributedThrottlerClientConfigPtr config,
    std::function<NRpc::IChannelPtr()> channelProvider,
    std::function<TPriority()> priorityProvider,
    IStatusProfilerPtr statusProfiler,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
