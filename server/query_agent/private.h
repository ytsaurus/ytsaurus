#pragma once

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger QueryAgentLogger;
extern const NProfiling::TProfiler QueryAgentProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT
