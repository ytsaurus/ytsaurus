#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, TabletBalancerLogger, "TabletBalancer");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
