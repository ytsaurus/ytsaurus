#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ClusterNodeLogger, "ClusterNode");
inline const NProfiling::TProfiler ClusterNodeProfiler("/cluster_node");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
