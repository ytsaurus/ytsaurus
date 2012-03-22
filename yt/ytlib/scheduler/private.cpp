#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

NLog::TLogger SchedulerLogger("Scheduler");
NProfiling::TProfiler SchedulerProfiler("scheduler_profiler");

NLog::TLogger SchedulerLogger("Operation");

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

