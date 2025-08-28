#pragma once

#include <yt/yt/server/scheduler/strategy/policy/scheduling_heartbeat_context.h>

#include <yt/yt/server/scheduler/common/public.h>

#include <yt/yt/server/lib/scheduler/public.h>

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

NPolicy::ISchedulingHeartbeatContextPtr CreateSchedulingHeartbeatContext(
    int nodeShardId,
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TAllocationPtr>& runningAllocations,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory,
    const TJobResources& defaultMinSpareAllocationResources);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
