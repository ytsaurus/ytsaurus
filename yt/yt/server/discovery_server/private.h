#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NClusterDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TClusterDiscoveryServerConfig)

struct IBootstrap;

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ClusterDiscoveryServerLogger;
extern const NProfiling::TProfiler ClusterDiscoveryServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterDiscoveryServer
