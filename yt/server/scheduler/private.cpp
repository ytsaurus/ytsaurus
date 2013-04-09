#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

const double ApproximateSizesBoostFactor = 1.3;

NLog::TLogger SchedulerLogger("Scheduler");
NLog::TLogger OperationLogger("Operation");
NProfiling::TProfiler SchedulerProfiler("/scheduler");

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

