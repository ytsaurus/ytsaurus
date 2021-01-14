#include "private.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger SchedulerEventLogger("SchedulerEventLog", /* essentital */ true);
const NLogging::TLogger SchedulerResourceMeteringLogger("SchedulerResourceMetering", /* essentital */ true);

const NProfiling::TRegistry SchedulerProfiler{"/scheduler"};

const TString EventLogPoolTreeKey{"tree_id"};
const TString ProfilingPoolTreeKey{"tree"};

const NLogging::TLogger NodeShardLogger("NodeShard");
const NLogging::TLogger StrategyLogger("Strategy");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

