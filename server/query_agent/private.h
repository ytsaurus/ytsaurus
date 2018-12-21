#pragma once

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger QueryAgentLogger;
extern const NProfiling::TProfiler QueryAgentProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
