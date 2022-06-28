#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger TabletBalancerLogger("TabletBalancer");
inline const NProfiling::TProfiler TabletBalancerProfiler("/tablet_balancer");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
