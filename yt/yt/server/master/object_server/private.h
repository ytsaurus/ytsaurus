#pragma once

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ObjectServerLogger;
extern const NProfiling::TProfiler ObjectServerProfiler;
extern const NProfiling::TRegistry ObjectServerProfilerRegistry;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

