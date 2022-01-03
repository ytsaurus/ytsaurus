#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger QueryAgentLogger("QueryAgent");
inline const NProfiling::TProfiler QueryAgentProfiler("/query_agent");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
