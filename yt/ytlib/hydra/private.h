#pragma once

#include "public.h"

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger HydraLogger;
extern const NProfiling::TProfiler HydraProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
