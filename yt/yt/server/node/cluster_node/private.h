#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ClusterNodeLogger;
extern const NProfiling::TProfiler ClusterNodeProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
