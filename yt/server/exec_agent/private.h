#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ExecAgentLogger;
extern const NProfiling::TProfiler ExecAgentProfiler;

extern const int TmpfsRemoveAttemptCount;

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

