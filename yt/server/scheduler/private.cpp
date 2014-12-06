#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

const double ApproximateSizesBoostFactor = 1.3;

const NLog::TLogger SchedulerLogger("Scheduler");
const NLog::TLogger OperationLogger("Operation");
NProfiling::TProfiler SchedulerProfiler("/scheduler");

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

