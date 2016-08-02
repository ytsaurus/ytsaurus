#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NJobAgent {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger JobTrackerServerLogger;
extern const NProfiling::TProfiler Profiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobAgent
} // namespace NYT
