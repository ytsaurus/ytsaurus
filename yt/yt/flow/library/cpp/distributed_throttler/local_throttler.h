#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

//! Tracks local quota usage and rejects calls once |failureFuture| contains a terminal error.
NConcurrency::IThroughputThrottlerPtr CreateLocalThrottler(
    NConcurrency::IThroughputThrottlerPtr underlying,
    TFuture<void> failureFuture,
    NProfiling::TProfiler profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
