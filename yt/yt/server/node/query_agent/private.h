#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/profiler.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger QueryAgentLogger;
extern const NProfiling::TRegistry QueryAgentProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
