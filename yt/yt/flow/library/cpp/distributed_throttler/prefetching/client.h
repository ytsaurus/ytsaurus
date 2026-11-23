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

//! Creates a prefetching throttler client.
//!
//! |priorityProvider| returns the priority key for each RequestQuota
//! (smaller == higher priority on the server queue).
//!
//! |statusProfiler| is optional and receives RPC-health reports.
//! |quotaClassProvider| returns the quota class for each RequestQuota; an
//! empty result means the reserved default class.
NConcurrency::IThroughputThrottlerPtr CreateDistributedThrottler(
    TDistributedThrottlerClientConfigPtr config,
    NRpc::IChannelPtr controllerChannel,
    std::function<TPriority()> priorityProvider,
    std::function<TQuotaClassId()> quotaClassProvider,
    IStatusProfilerPtr statusProfiler,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
