#include "private.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger SchedulerEventLogger("SchedulerEventLog", /* essentital */ true);
const NLogging::TLogger SchedulerResourceMeteringLogger("SchedulerResourceMetering", /* essentital */ true);

const NProfiling::TProfiler SchedulerProfiler("/scheduler");
const NProfiling::TRegistry SchedulerProfilerRegistry{"/scheduler"};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

