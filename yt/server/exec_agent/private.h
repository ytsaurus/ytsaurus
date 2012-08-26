#pragma once

#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////

extern NLog::TLogger ExecAgentLogger;
extern NProfiling::TProfiler ExecAgentProfiler;

extern const Stroka SandboxName;
extern const Stroka ProxyConfigFileName;

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

