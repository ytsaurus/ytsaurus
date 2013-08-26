#pragma once

#include <ytlib/logging/log.h>

#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ElectionLogger;
extern NProfiling::TProfiler ElectionProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
