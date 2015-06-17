#pragma once

#include <core/logging/log.h>
#include <core/profiling/profiler.h>

namespace NYT {
namespace NQueryAgent {

////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger QueryAgentLogger;
extern const NProfiling::TProfiler QueryAgentProfiler;

////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT
