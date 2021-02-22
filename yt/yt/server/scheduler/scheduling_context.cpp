#include "scheduling_context.h"
#include "scheduling_context_detail.h"

namespace NYT::NScheduler {

using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TSchedulingContext
    : public TSchedulingContextBase
{
public:
    TSchedulingContext(
        int nodeShardId,
        TSchedulerConfigPtr config,
        TExecNodePtr node,
        const std::vector<TJobPtr>& runningJobs,
        const NChunkClient::TMediumDirectoryPtr& mediumDirectory)
        : TSchedulingContextBase(
            nodeShardId,
            std::move(config),
            std::move(node),
            runningJobs,
            mediumDirectory)
    { }

    virtual NProfiling::TCpuInstant GetNow() const override
    {
        return NProfiling::GetCpuInstant();
    }
};

void Serialize(const TScheduleJobsStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .Item("preemptable_job_count").Value(statistics.PreemptableJobCount)
        .Item("controller_schedule_job_count").Value(statistics.ControllerScheduleJobCount)
        .Item("non_preemptive_schedule_job_attempts").Value(statistics.NonPreemptiveScheduleJobAttempts)
        .Item("aggressively_preemptive_schedule_job_attempts").Value(statistics.AggressivelyPreemptiveScheduleJobAttempts)
        .Item("preemptive_schedule_job_attempts").Value(statistics.PreemptiveScheduleJobAttempts)
        .Item("has_aggressively_starving_elements").Value(statistics.HasAggressivelyStarvingElements)
        .Item("resource_usage_discount").Value(statistics.ResourceUsageDiscount)
        .Item("resource_usage").Value(statistics.ResourceUsage)
        .Item("resource_limits").Value(statistics.ResourceLimits)
    .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

ISchedulingContextPtr CreateSchedulingContext(
    int nodeShardId,
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TJobPtr>& runningJobs,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory)
{
    return New<TSchedulingContext>(
        nodeShardId,
        std::move(config),
        std::move(node),
        runningJobs,
        mediumDirectory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
