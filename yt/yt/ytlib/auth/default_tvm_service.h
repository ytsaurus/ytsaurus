#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/profiling/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

ITvmServicePtr CreateDefaultTvmService(
    TDefaultTvmServiceConfigPtr config,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
