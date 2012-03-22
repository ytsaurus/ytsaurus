#pragma once

#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

extern NLog::TLogger SchedulerLogger;
extern NProfiling::TProfiler SchedulerProfiler;

extern NLog::TLogger OperationLogger;

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

