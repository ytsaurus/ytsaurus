#pragma once

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ElectionLogger;
extern NProfiling::TProfiler ElectionProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
