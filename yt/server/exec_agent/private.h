#pragma once

#include <core/logging/log.h>
#include <core/profiling/profiler.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////

extern const NLog::TLogger ExecAgentLogger;
extern NProfiling::TProfiler ExecAgentProfiler;

extern const Stroka ProxyConfigFileName;

extern const Stroka SandboxDirectoryName;

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

