#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/misc/global.h>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, YqlAgentLogger, "YqlAgent");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, YqlAgentProfiler, NProfiling::TProfiler("/yql_agent").WithGlobal());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
