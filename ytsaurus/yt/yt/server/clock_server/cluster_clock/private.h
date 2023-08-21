#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NClusterClock {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger ClusterClockLogger("Clock");
inline const NProfiling::TProfiler ClusterClockProfiler("/clock");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
