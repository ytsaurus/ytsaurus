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

    // TODO(eshcherbin): Remove this method, as it is misleading.
    NProfiling::TCpuInstant GetNow() const override
    {
        return NProfiling::GetCpuInstant();
    }
};

void Serialize(const TScheduleJobsStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .Item("unconditionally_preemptible_job_count").Value(statistics.UnconditionallyPreemptibleJobCount)
        .Item("controller_schedule_job_count").Value(statistics.ControllerScheduleJobCount)
        .Item("controller_schedule_job_timed_out_count").Value(statistics.ControllerScheduleJobTimedOutCount)
        .Item("schedule_job_attempt_count_per_stage").Value(statistics.ScheduleJobAttemptCountPerStage)
        .Item("operation_count_by_preemption_priority").Value(statistics.OperationCountByPreemptionPriority)
        .Item("unconditional_resource_usage_discount").Value(statistics.UnconditionalResourceUsageDiscount)
        .Item("resource_usage").Value(statistics.ResourceUsage)
        .Item("resource_limits").Value(statistics.ResourceLimits)
        .Item("ssd_priority_preemption_enabled").Value(statistics.SsdPriorityPreemptionEnabled)
        .Item("ssd_priority_preemption_media").Value(statistics.SsdPriorityPreemptionMedia)
    .EndMap();
}

TString FormatPreemptibleInfoCompact(const TScheduleJobsStatistics& statistics)
{
    return Format("{UJC: %v, UD: %v, TCJC: %v, MCJCPP: %v, MCD: %v}",
        statistics.UnconditionallyPreemptibleJobCount,
        FormatResources(statistics.UnconditionalResourceUsageDiscount),
        statistics.TotalConditionallyPreemptibleJobCount,
        statistics.MaxConditionallyPreemptibleJobCountInPool,
        FormatResources(statistics.MaxConditionalResourceUsageDiscount));
}

TString FormatScheduleJobAttemptsCompact(const TScheduleJobsStatistics& statistics)
{
    return Format("{RM: %v, RL: %v, PSA: %v, PSN: %v, PA: %v, PN: %v, C: %v, TO: %v, MNPSI: %v}",
        statistics.ScheduleJobAttemptCountPerStage[EJobSchedulingStage::RegularMediumPriority],
        statistics.ScheduleJobAttemptCountPerStage[EJobSchedulingStage::RegularLowPriority],
        statistics.ScheduleJobAttemptCountPerStage[EJobSchedulingStage::PreemptiveSsdAggressive],
        statistics.ScheduleJobAttemptCountPerStage[EJobSchedulingStage::PreemptiveSsdNormal],
        statistics.ScheduleJobAttemptCountPerStage[EJobSchedulingStage::PreemptiveAggressive],
        statistics.ScheduleJobAttemptCountPerStage[EJobSchedulingStage::PreemptiveNormal],
        statistics.ControllerScheduleJobCount,
        statistics.ControllerScheduleJobTimedOutCount,
        statistics.MaxNonPreemptiveSchedulingIndex);
}

TString FormatOperationCountByPreemptionPriorityCompact(
    const TEnumIndexedVector<EOperationPreemptionPriority, int>& operationCountByPriority)
{
    return Format("{N: %v, R: %v, A: %v, SR: %v, SA: %v}",
        operationCountByPriority[EOperationPreemptionPriority::None],
        operationCountByPriority[EOperationPreemptionPriority::Normal],
        operationCountByPriority[EOperationPreemptionPriority::Aggressive],
        operationCountByPriority[EOperationPreemptionPriority::SsdNormal],
        operationCountByPriority[EOperationPreemptionPriority::SsdAggressive]);
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
