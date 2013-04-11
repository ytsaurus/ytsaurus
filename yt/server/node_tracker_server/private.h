#pragma once

#include "public.h"

#include <ytlib/logging/log.h>

#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger NodeTrackerServerLogger;
extern NProfiling::TProfiler NodeTrackerServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
