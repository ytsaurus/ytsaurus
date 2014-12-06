#pragma once

#include "public.h"

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLog::TLogger NodeTrackerServerLogger;
extern NProfiling::TProfiler NodeTrackerServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
