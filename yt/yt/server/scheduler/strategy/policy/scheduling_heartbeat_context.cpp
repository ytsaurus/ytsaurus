#include "scheduling_heartbeat_context.h"

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

namespace NYT::NScheduler::NStrategy::NPolicy {

using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TScheduleAllocationsStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .Item("preemptible_job_count").Value(statistics.PreemptibleAllocationCount)
        .Item("controller_schedule_job_count").Value(statistics.ControllerScheduleAllocationCount)
        .Item("controller_schedule_job_timed_out_count").Value(statistics.ControllerScheduleAllocationTimedOutCount)
        .Item("schedule_job_attempt_count_per_stage").Value(statistics.ScheduleAllocationAttemptCountPerStage)
        .Item("operation_count_by_preemption_priority").Value(statistics.OperationCountByPreemptionPriority)
        .Item("resource_usage_discount").Value(statistics.ResourceUsageDiscount)
        .Item("resource_usage").Value(statistics.ResourceUsage)
        .Item("resource_limits").Value(statistics.ResourceLimits)
        .Item("ssd_priority_preemption_enabled").Value(statistics.SsdPriorityPreemptionEnabled)
        .Item("ssd_priority_preemption_media").Value(statistics.SsdPriorityPreemptionMedia)
    .EndMap();
}

TString FormatScheduleAllocationAttemptsCompact(const TScheduleAllocationsStatistics& statistics)
{
    return Format("{RH: %v, RM: %v, PSA: %v, PSN: %v, PA: %v, PN: %v, C: %v, TO: %v, MNPSI: %v}",
        statistics.ScheduleAllocationAttemptCountPerStage[EAllocationSchedulingStage::RegularHighPriority],
        statistics.ScheduleAllocationAttemptCountPerStage[EAllocationSchedulingStage::RegularMediumPriority],
        statistics.ScheduleAllocationAttemptCountPerStage[EAllocationSchedulingStage::PreemptiveSsdAggressive],
        statistics.ScheduleAllocationAttemptCountPerStage[EAllocationSchedulingStage::PreemptiveSsdNormal],
        statistics.ScheduleAllocationAttemptCountPerStage[EAllocationSchedulingStage::PreemptiveAggressive],
        statistics.ScheduleAllocationAttemptCountPerStage[EAllocationSchedulingStage::PreemptiveNormal],
        statistics.ControllerScheduleAllocationCount,
        statistics.ControllerScheduleAllocationTimedOutCount,
        statistics.MaxNonPreemptiveSchedulingIndex);
}

TString FormatOperationCountByPreemptionPriorityCompact(
    const TEnumIndexedArray<EOperationPreemptionPriority, int>& operationCountByPriority)
{
    return Format("{N: %v, R: %v, A: %v, SR: %v, SA: %v}",
        operationCountByPriority[EOperationPreemptionPriority::None],
        operationCountByPriority[EOperationPreemptionPriority::Normal],
        operationCountByPriority[EOperationPreemptionPriority::Aggressive],
        operationCountByPriority[EOperationPreemptionPriority::SsdNormal],
        operationCountByPriority[EOperationPreemptionPriority::SsdAggressive]);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
