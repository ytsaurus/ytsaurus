#include "scheduling_heartbeat_context.h"

#include "scheduling_heartbeat_context_detail.h"

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NScheduler::NStrategy {

using namespace NPolicy;

////////////////////////////////////////////////////////////////////////////////

class TSchedulingHeartbeatContext
    : public TSchedulingHeartbeatContextBase
{
public:
    TSchedulingHeartbeatContext(
        int nodeShardId,
        TSchedulerConfigPtr config,
        TExecNodePtr node,
        const std::vector<TAllocationPtr>& runningAllocations,
        const NChunkClient::TMediumDirectoryPtr& mediumDirectory,
        const TJobResources& defaultMinSpareAllocationResources)
        : TSchedulingHeartbeatContextBase(
            nodeShardId,
            std::move(config),
            std::move(node),
            runningAllocations,
            mediumDirectory,
            defaultMinSpareAllocationResources)
    { }

    NProfiling::TCpuInstant GetNow() const override
    {
        return NProfiling::GetCpuInstant();
    }
};

ISchedulingHeartbeatContextPtr CreateSchedulingHeartbeatContext(
    int nodeShardId,
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TAllocationPtr>& runningAllocations,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory,
    const TJobResources& defaultMinSpareAllocationResources)
{
    return New<TSchedulingHeartbeatContext>(
        nodeShardId,
        std::move(config),
        std::move(node),
        runningAllocations,
        mediumDirectory,
        defaultMinSpareAllocationResources);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
