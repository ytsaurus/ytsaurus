#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger NodeTrackerServerLogger;
extern const NProfiling::TProfiler NodeTrackerServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
