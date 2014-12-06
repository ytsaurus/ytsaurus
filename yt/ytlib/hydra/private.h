#pragma once

#include "public.h"

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

extern const NLog::TLogger HydraLogger;
extern NProfiling::TProfiler HydraProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
