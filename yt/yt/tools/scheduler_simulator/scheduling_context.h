#pragma once

#include "scheduling_strategy_host.h"

#include <yt/yt/server/scheduler/scheduling_context_detail.h>
#include <yt/yt/server/scheduler/exec_node.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

class TSchedulingContext
    : public NScheduler::TSchedulingContextBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NProfiling::TCpuInstant, Now);

public:
    TSchedulingContext(
        int shardId,
        NScheduler::TSchedulerConfigPtr schedulerConfig,
        NScheduler::TExecNodePtr node,
        const std::vector<NScheduler::TAllocationPtr>& runningAllocations,
        const NChunkClient::TMediumDirectoryPtr& mediumDirectory,
        const NScheduler::TJobResources& defaultMinSpareAllocationResources)
        : TSchedulingContextBase(
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
