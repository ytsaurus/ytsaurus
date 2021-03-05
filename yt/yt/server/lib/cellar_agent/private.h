#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger CellarAgentLogger;
extern const NProfiling::TRegistry CellarAgentProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
