#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NClusterClock {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ClusterClockLogger;
extern const NProfiling::TRegistry ClusterClockProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
