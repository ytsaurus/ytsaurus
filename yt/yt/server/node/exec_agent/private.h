#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ExecAgentLogger;
extern const NProfiling::TProfiler ExecAgentProfiler;

extern const int TmpfsRemoveAttemptCount;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
