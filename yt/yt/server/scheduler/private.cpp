#include "private.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger SchedulerLogger("Scheduler");
const NLogging::TLogger SchedulerEventLogger("SchedulerEventLog", /* essentital */ true);
const NLogging::TLogger SchedulerResourceMeteringLogger("SchedulerResourceMetering", /* essentital */ true);

const NProfiling::TProfiler SchedulerProfiler("/scheduler");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

