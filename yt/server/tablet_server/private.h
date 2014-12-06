#pragma once

#include "public.h"

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletTracker;
typedef TIntrusivePtr<TTabletTracker> TTabletTrackerPtr;

////////////////////////////////////////////////////////////////////////////////

extern const NLog::TLogger TabletServerLogger;
extern NProfiling::TProfiler TabletServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
