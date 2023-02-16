#include "fair_share_tree_job_scheduler_structs.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NScheduler {

using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TRunningJobStatistics& statistics, TStringBuf /*format*/)
{
    builder->AppendFormat("{TotalCpuTime: %v, PreemptibleCpuTime: %v, TotalGpuTime: %v, PreemptibleGpuTime: %v}",
        statistics.TotalCpuTime,
        statistics.PreemptibleCpuTime,
        statistics.TotalGpuTime,
        statistics.PreemptibleGpuTime);
}

TString ToString(const TRunningJobStatistics& statistics)
{
    return ToStringViaBuilder(statistics);
}

TString FormatRunningJobStatisticsCompact(const TRunningJobStatistics& statistics)
{
    return Format("{TCT: %v, PCT: %v, TGT: %v, PGT: %v}",
        statistics.TotalCpuTime,
        statistics.PreemptibleCpuTime,
        statistics.TotalGpuTime,
        statistics.PreemptibleGpuTime);
}

void Serialize(const TRunningJobStatistics& statistics, NYson::IYsonConsumer* consumer)
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

TFairShareTreeJobSchedulerOperationState::TFairShareTreeJobSchedulerOperationState(
    TStrategyOperationSpecPtr spec,
    bool isGang)
    : Spec(std::move(spec))
    , IsGang(isGang)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
