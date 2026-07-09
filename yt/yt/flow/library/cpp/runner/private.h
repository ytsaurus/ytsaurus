#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, NodeLogger, "FlowNode");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, NodeProfiler, "/flow/node");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
