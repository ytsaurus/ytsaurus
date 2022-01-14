#include "scheduling_context.h"
#include "scheduling_context_detail.h"

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/core/profiling/timing.h>

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

    NProfiling::TCpuInstant GetNow() const override
    {
        return NProfiling::GetCpuInstant();
    }
};

void Serialize(const TScheduleJobsStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .Item("unconditionally_preemptable_job_count").Value(statistics.UnconditionallyPreemptableJobCount)
        .Item("controller_schedule_job_count").Value(statistics.ControllerScheduleJobCount)
        .Item("controller_schedule_job_timed_out_count").Value(statistics.ControllerScheduleJobTimedOutCount)
        .Item("schedule_job_attempt_count_per_stage").Value(statistics.ScheduleJobAttemptCountPerStage)
        .Item("operation_count_by_preemption_priority").Value(statistics.OperationCountByPreemptionPriority)
        .Item("unconditional_resource_usage_discount").Value(statistics.UnconditionalResourceUsageDiscount)
        .Item("resource_usage").Value(statistics.ResourceUsage)
        .Item("resource_limits").Value(statistics.ResourceLimits)
    .EndMap();
}

TString FormatPreemptableInfoCompact(const TScheduleJobsStatistics& statistics)
{
    return Format("{UJC: %v, UD: %v, TCJC: %v, MCJCPP: %v, MCD: %v}",
        statistics.UnconditionallyPreemptableJobCount,
        FormatResources(statistics.UnconditionalResourceUsageDiscount),
        statistics.TotalConditionallyPreemptableJobCount,
        statistics.MaxConditionallyPreemptableJobCountInPool,
        FormatResources(statistics.MaxConditionalResourceUsageDiscount));
}

TString FormatScheduleJobAttemptsCompact(const TScheduleJobsStatistics& statistics)
{
    return Format("{NP: %v, AP: %v, P: %v, C: %v, TO: %v, MNPSI: %v}",
        statistics.ScheduleJobAttemptCountPerStage[EJobSchedulingStage::NonPreemptive],
        statistics.ScheduleJobAttemptCountPerStage[EJobSchedulingStage::AggressivelyPreemptive],
        statistics.ScheduleJobAttemptCountPerStage[EJobSchedulingStage::Preemptive],
        statistics.ControllerScheduleJobCount,
        statistics.ControllerScheduleJobTimedOutCount,
        statistics.MaxNonPreemptiveSchedulingIndex);
}

TString FormatOperationCountByPreemptionPriorityCompact(const TScheduleJobsStatistics& statistics)
{
    return Format("{R: %v, A: %v}",
        statistics.OperationCountByPreemptionPriority[EOperationPreemptionPriority::Regular],
        statistics.OperationCountByPreemptionPriority[EOperationPreemptionPriority::Aggressive]);
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
