#pragma once

#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////

extern NLog::TLogger ExecAgentLogger;
extern NProfiling::TProfiler ExecAgentProfiler;

extern const Stroka SandboxDirectoryName;
extern const Stroka ProxyConfigFileName;

extern const int EmptyUserId;

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

