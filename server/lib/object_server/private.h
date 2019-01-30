#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ObjectServerLogger;
extern const NProfiling::TProfiler ObjectServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

