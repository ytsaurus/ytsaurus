#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NClusterDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TClusterDiscoveryServerConfig)

struct IBootstrap;

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger ClusterDiscoveryServerLogger("DiscoveryServer");
inline const NProfiling::TProfiler ClusterDiscoveryServerProfiler("/discovery_server");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterDiscoveryServer
