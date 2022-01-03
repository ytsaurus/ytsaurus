#pragma once

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCellBalancerBootstrapConfig)
DECLARE_REFCOUNTED_CLASS(TCellBalancerConfig)
DECLARE_REFCOUNTED_CLASS(TCellBalancerMasterConnectorConfig)

struct IBootstrap;

DECLARE_REFCOUNTED_CLASS(IMasterConnector)
DECLARE_REFCOUNTED_CLASS(ICellTracker)
DECLARE_REFCOUNTED_CLASS(TCellTrackerImpl)

DECLARE_REFCOUNTED_CLASS(TClusterStateProvider)

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger CellBalancerLogger("CellBalancer");
inline const NProfiling::TProfiler CellBalancerProfiler("/cell_balancer");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
