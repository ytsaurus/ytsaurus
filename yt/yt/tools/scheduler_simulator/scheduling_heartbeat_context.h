#pragma once

#include "scheduling_strategy_host.h"

#include <yt/yt/server/scheduler/strategy/scheduling_heartbeat_context_detail.h>

#include <yt/yt/server/scheduler/common/exec_node.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Split into header and implementation.
class TSchedulingHeartbeatContext
    : public NScheduler::NStrategy::TSchedulingHeartbeatContextBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NProfiling::TCpuInstant, Now);

public:
    TSchedulingHeartbeatContext(
        int shardId,
        NScheduler::TSchedulerConfigPtr schedulerConfig,
        NScheduler::TExecNodePtr node,
        const std::vector<NScheduler::TAllocationPtr>& runningAllocations,
        const NChunkClient::TMediumDirectoryPtr& mediumDirectory,
        const NScheduler::TJobResources& defaultMinSpareAllocationResources)
        : TSchedulingHeartbeatContextBase(
            shardId,
            schedulerConfig,
            node,
            runningAllocations,
            mediumDirectory,
            defaultMinSpareAllocationResources)
    { }

    void SetDurationForStartedAllocation(NScheduler::TAllocationId allocationId, const TDuration& duration)
    {
        Durations_[allocationId] = duration;
    }

    const THashMap<NScheduler::TAllocationId, TDuration>& GetStartedAllocationsDurations() const
    {
        return Durations_;
    }

private:
    THashMap<NScheduler::TAllocationId, TDuration> Durations_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
