#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ExecAgentLogger;
extern const NProfiling::TProfiler ExecAgentProfiler;

extern const int TmpfsRemoveAttemptCount;

////////////////////////////////////////////////////////////////////////////////

TString GetJobProxyUnixDomainName(const TString& nodeTag, int slotIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent

