#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ChaosNodeLogger, "ChaosNode");
inline const NProfiling::TRegistry ChaosNodeProfiler("/chaos_node");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
