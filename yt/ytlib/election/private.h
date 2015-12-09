#pragma once

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ElectionLogger;
extern const NProfiling::TProfiler ElectionProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
