#pragma once

#include "public.h"

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

ITvmServicePtr CreateCachingTvmService(
    ITvmServicePtr underlying,
    TAsyncExpiringCacheConfigPtr config,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
