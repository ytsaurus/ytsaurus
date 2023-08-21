#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger NodeTrackerServerStructuredLogger("NodeTrackerServerStructured");
inline const NLogging::TLogger NodeTrackerServerLogger("NodeTrackerServer");
inline const NProfiling::TProfiler NodeTrackerProfiler("/node_tracker");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
