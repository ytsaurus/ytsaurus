#pragma once

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ExecAgentLogger;
extern const NProfiling::TProfiler ExecAgentProfiler;

extern const Stroka TmpfsDirName;

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

