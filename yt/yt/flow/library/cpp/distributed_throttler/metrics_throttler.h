#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

//! Thin IThroughputThrottler wrapper that records local consumption metrics.
//!
//! TPrefetchingThrottler exposes no metrics itself, and TRemoteThrottler only
//! sees prefetch RPCs — neither reflects what the consumer actually draws.
//! Placing this wrapper on top of the client stack gives per-client Solomon
//! series that mirror what the computation is consuming in real time.
//!
//! Recorded:
//! - `/consumed` — units granted through any successful path
//!   (Throttle/TryAcquire/TryAcquireAvailable/Acquire).
//! - `/released` — units passed to Release.
//! - `/wait_time` — histogram of Throttle() wait durations.
NConcurrency::IThroughputThrottlerPtr CreateMetricsTrackingThrottler(
    NConcurrency::IThroughputThrottlerPtr underlying,
    NProfiling::TProfiler profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
