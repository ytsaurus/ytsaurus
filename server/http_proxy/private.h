#pragma once

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger HttpProxyLogger;
extern const NProfiling::TProfiler HttpProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
