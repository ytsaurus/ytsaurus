#pragma once

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTabletBalancerConfig)
DECLARE_REFCOUNTED_CLASS(TTabletBalancerServerConfig)

struct IBootstrap;

DECLARE_REFCOUNTED_STRUCT(ITabletBalancer)

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger TabletBalancerLogger("TabletBalancer");
inline const NProfiling::TProfiler TabletBalancerProfiler("/tablet_balancer");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
