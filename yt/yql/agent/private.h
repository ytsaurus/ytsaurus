#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/misc/leaky_global.h>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, YqlAgentLogger, "YqlAgent");
YT_DEFINE_LEAKY_GLOBAL(const NProfiling::TProfiler, YqlAgentProfiler, NProfiling::TProfiler("/yql_agent"));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
