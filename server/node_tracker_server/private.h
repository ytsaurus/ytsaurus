#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger NodeTrackerServerLogger;
extern const NProfiling::TProfiler NodeTrackerServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
