#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, TabletBalancerLogger, "TabletBalancer");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, TabletBalancerProfiler, "/tablet_balancer");

static const NYPath::TYPath DefaultTabletBalancerRootPath = "//sys/tablet_balancer";
static const NYPath::TYPath DefaultTabletBalancerDynamicConfigPath = DefaultTabletBalancerRootPath + "/config";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
