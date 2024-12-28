#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NClusterDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDiscoveryServerBootstrapConfig)
DECLARE_REFCOUNTED_CLASS(TDiscoveryServerProgramConfig)

DECLARE_REFCOUNTED_STRUCT(IBootstrap)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ClusterDiscoveryServerLogger, "DiscoveryServer");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, ClusterDiscoveryServerProfiler, "/discovery_server");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterDiscoveryServer
