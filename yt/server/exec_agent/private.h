#pragma once

#include <core/logging/log.h>
#include <core/profiling/profiler.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ExecAgentLogger;
extern const NProfiling::TProfiler ExecAgentProfiler;

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

