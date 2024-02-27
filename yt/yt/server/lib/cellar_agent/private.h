#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger CellarAgentLogger("CellarAgent");
inline const NProfiling::TProfiler CellarAgentProfiler("/cellar_agent");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
