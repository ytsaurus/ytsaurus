#pragma once

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NClusterClock {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ClusterClockLogger;
extern const NProfiling::TProfiler ClusterClockProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
