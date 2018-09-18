#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger HydraLogger;
extern const NProfiling::TProfiler HydraProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
