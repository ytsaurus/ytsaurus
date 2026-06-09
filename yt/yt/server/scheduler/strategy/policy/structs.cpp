#include "structs.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NScheduler::NStrategy::NPolicy {

using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TRunningAllocationStatistics& statistics, TStringBuf /*spec*/)
{
    builder->AppendFormat(
        "{TotalCpuTime: %v, PreemptibleCpuTime: %v, TotalGpuTime: %v, PreemptibleGpuTime: %v}",
        statistics.TotalCpuTime,
        statistics.PreemptibleCpuTime,
        statistics.TotalGpuTime,
        statistics.PreemptibleGpuTime);
}

TString FormatRunningAllocationStatisticsCompact(const TRunningAllocationStatistics& statistics)
{
    return Format(
        "{TCT: %v, PCT: %v, TGT: %v, PGT: %v}",
        statistics.TotalCpuTime,
        statistics.PreemptibleCpuTime,
        statistics.TotalGpuTime,
        statistics.PreemptibleGpuTime);
}

void Serialize(const TRunningAllocationStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("total_cpu_time").Value(statistics.TotalCpuTime)
            .Item("preemptible_cpu_time").Value(statistics.PreemptibleCpuTime)
            .Item("total_gpu_time").Value(statistics.TotalGpuTime)
            .Item("preemptible_gpu_time").Value(statistics.PreemptibleGpuTime)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TScheduleAllocationsStatisticsImplPtr& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Do(std::bind(BuildScheduleAllocationsStatisticsCommon, statistics, std::placeholders::_1))
            .Item("schedule_job_attempt_count_per_stage").Value(statistics->ScheduleAllocationAttemptCountPerStage)
            .Item("operation_count_by_preemption_priority").Value(statistics->OperationCountByPreemptionPriority)
            .Item("resource_usage_discount").Value(statistics->ResourceUsageDiscount)
            .Item("ssd_priority_preemption_enabled").Value(statistics->SsdPriorityPreemptionEnabled)
            .Item("ssd_priority_preemption_media").Value(statistics->SsdPriorityPreemptionMedia)
        .EndMap();
}

TString TScheduleAllocationsStatisticsImpl::FormatScheduleAllocationAttemptsCompact() const
{
    return Format("{RH: %v, RM: %v, PSA: %v, PSN: %v, PA: %v, PN: %v, C: %v, TO: %v, MNPSI: %v}",
        ScheduleAllocationAttemptCountPerStage[EAllocationSchedulingStage::RegularHighPriority],
        ScheduleAllocationAttemptCountPerStage[EAllocationSchedulingStage::RegularMediumPriority],
        ScheduleAllocationAttemptCountPerStage[EAllocationSchedulingStage::PreemptiveSsdAggressive],
        ScheduleAllocationAttemptCountPerStage[EAllocationSchedulingStage::PreemptiveSsdNormal],
        ScheduleAllocationAttemptCountPerStage[EAllocationSchedulingStage::PreemptiveAggressive],
        ScheduleAllocationAttemptCountPerStage[EAllocationSchedulingStage::PreemptiveNormal],
        ControllerScheduleAllocationCount,
        ControllerScheduleAllocationTimedOutCount,
        MaxNonPreemptiveSchedulingIndex);
}

TString TScheduleAllocationsStatisticsImpl::FormatOperationCountByPreemptionPriorityCompact() const
{
    return Format("{N: %v, R: %v, A: %v, SR: %v, SA: %v, GFH: %v}",
        OperationCountByPreemptionPriority[EOperationPreemptionPriority::None],
        OperationCountByPreemptionPriority[EOperationPreemptionPriority::Normal],
        OperationCountByPreemptionPriority[EOperationPreemptionPriority::Aggressive],
        OperationCountByPreemptionPriority[EOperationPreemptionPriority::SsdNormal],
        OperationCountByPreemptionPriority[EOperationPreemptionPriority::SsdAggressive],
        OperationCountByPreemptionPriority[EOperationPreemptionPriority::DefaultGpuFullHost]);
}

////////////////////////////////////////////////////////////////////////////////

TOperationState::TOperationState(
    TStrategyOperationSpecPtr spec,
    bool isGang)
    : Spec(std::move(spec))
    , IsGang(isGang)
{ }

////////////////////////////////////////////////////////////////////////////////

void TAllocationState::Register(TRegistrar registrar)
{
    registrar.Parameter("operation_id", &TThis::OperationId)
        .Default();

    registrar.Parameter("resource_limits", &TThis::ResourceLimits)
        .Default();

    registrar.Parameter("preemption_status", &TThis::PreemptionStatus)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
