#include "private.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger SchedulerEventLogger("SchedulerEventLog", /* essentital */ true);
const NLogging::TLogger SchedulerResourceMeteringLogger("SchedulerResourceMetering", /* essentital */ true);

const NProfiling::TRegistry SchedulerProfiler{"/scheduler"};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

