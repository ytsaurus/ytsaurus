#include "private.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger SchedulerLogger("Scheduler");
const NLogging::TLogger SchedulerEventLogger("SchedulerEventLog");
const NLogging::TLogger SchedulerResourceMeteringLogger("SchedulerResourceMetering");

const NProfiling::TProfiler SchedulerProfiler("/scheduler");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

