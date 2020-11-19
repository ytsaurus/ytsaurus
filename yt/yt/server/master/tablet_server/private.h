#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTabletTracker)

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger TabletServerLogger;
extern const NProfiling::TRegistry TabletServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
