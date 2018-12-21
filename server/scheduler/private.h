#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

extern const NYT::NLogging::TLogger Logger;
extern const NYT::NProfiling::TProfiler Profiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
