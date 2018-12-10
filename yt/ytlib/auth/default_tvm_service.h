#pragma once

#include "public.h"

#include <yt/core/concurrency/public.h>

#include <yt/core/profiling/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

ITvmServicePtr CreateDefaultTvmService(
    TDefaultTvmServiceConfigPtr config,
    NConcurrency::IPollerPtr poller,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
