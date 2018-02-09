#include "private.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

const TString RootPoolName = "<Root>";
const TString DefaultTreeAttributeName = "default_tree";

const NLogging::TLogger SchedulerLogger("Scheduler");
const NProfiling::TProfiler SchedulerProfiler("/scheduler");

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

