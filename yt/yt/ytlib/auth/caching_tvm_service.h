#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

ITvmServicePtr CreateCachingTvmService(
    ITvmServicePtr underlying,
    TAsyncExpiringCacheConfigPtr config,
    NProfiling::TRegistry profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
