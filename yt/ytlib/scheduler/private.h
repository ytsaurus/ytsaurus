#pragma once

#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

extern NLog::TLogger SchedulerLogger;
extern NLog::TLogger OperationsLogger;
extern NProfiling::TProfiler SchedulerProfiler;

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

