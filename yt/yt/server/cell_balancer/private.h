#pragma once

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCellBalancerBootstrapConfig)
DECLARE_REFCOUNTED_STRUCT(TCellBalancerProgramConfig)
DECLARE_REFCOUNTED_STRUCT(TCellBalancerConfig)
DECLARE_REFCOUNTED_STRUCT(TBundleControllerConfig)
DECLARE_REFCOUNTED_STRUCT(TChaosConfig)

DECLARE_REFCOUNTED_STRUCT(IBootstrap)
DECLARE_REFCOUNTED_STRUCT(ICellTracker)
DECLARE_REFCOUNTED_CLASS(TCellTrackerImpl)
DECLARE_REFCOUNTED_STRUCT(IBundleController)
DECLARE_REFCOUNTED_CLASS(TClusterStateProvider)
DECLARE_REFCOUNTED_STRUCT(ICellDowntimeTracker)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, CellBalancerLogger, "CellBalancer");
YT_DEFINE_GLOBAL(const NLogging::TLogger, BundleControllerLogger, "BundleController");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, CellBalancerProfiler, "/cell_balancer");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
