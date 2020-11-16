#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger NodeTrackerServerLogger;
extern const NProfiling::TRegistry NodeTrackerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
