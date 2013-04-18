#pragma once

#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////

extern NLog::TLogger ExecAgentLogger;
extern NProfiling::TProfiler ExecAgentProfiler;

extern const Stroka ProxyConfigFileName;

extern const Stroka SandboxDirectoryName;

extern const int EmptyUserId;

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

