#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

NLog::TLogger SchedulerLogger("Scheduler");
NLog::TLogger OperationsLogger("Operations");
NProfiling::TProfiler SchedulerProfiler("/scheduler");

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

