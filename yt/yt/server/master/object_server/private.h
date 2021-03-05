#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/profiler.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ObjectServerLogger;
extern const NProfiling::TRegistry ObjectServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

