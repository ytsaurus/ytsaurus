#pragma once

#include <yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ClusterNodeLogger;
extern const NProfiling::TRegistry ClusterNodeProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
