#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, TabletBalancerLogger, "TabletBalancer");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, TabletBalancerProfiler, "/tablet_balancer");

static const NYPath::TYPath DefaultTabletBalancerRootPath = "//sys/tablet_balancer";
static const NYPath::TYPath DefaultTabletBalancerDynamicConfigPath = DefaultTabletBalancerRootPath + "/config";

static const NYPath::TYPath TabletCellBundlesPath("//sys/tablet_cell_bundles");

// TODO(alexelex): rename TabletStaticPath to TabletStaticMemory.
static const TString TabletStaticPath = "/statistics/memory/tablet_static";
static const TString TabletSlotsPath = "/tablet_slots";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
