#pragma once

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

extern const NLog::TLogger ElectionLogger;
extern NProfiling::TProfiler ElectionProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
