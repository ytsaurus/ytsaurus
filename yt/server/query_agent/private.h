#pragma once

#include <core/logging/log.h>
#include <core/profiling/profiler.h>

namespace NYT {
namespace NQueryAgent {

////////////////////////////////////////////////////////////////////

extern const NLog::TLogger QueryAgentLogger;
extern NProfiling::TProfiler QueryAgentProfiler;

////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT
