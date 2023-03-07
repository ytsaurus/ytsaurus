#include "job_metrics.h"

#include <yt/server/lib/controller_agent/serialize.h>

#include <yt/ytlib/controller_agent/proto/controller_agent_service.pb.h>

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/metrics_accumulator.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/statistics.h>

#include <util/generic/cast.h>

namespace NYT::NScheduler {

using namespace NProfiling;
using namespace NJobTrackerClient;
using namespace NPhoenix;
using namespace NYTree;
using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

void TCustomJobMetricDescription::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, StatisticsPath);
    Persist(context, ProfilingName);
    Persist(context, AggregateType);

    if (context.GetVersion() >= ToUnderlying(NControllerAgent::ESnapshotVersion::JobMetricsJobStateFilter)) {
        Persist(context, JobStateFilter);
    }
}

bool operator==(const TCustomJobMetricDescription& lhs, const TCustomJobMetricDescription& rhs)
{
    return lhs.StatisticsPath == rhs.StatisticsPath &&
        lhs.ProfilingName == rhs.ProfilingName;
}

bool operator<(const TCustomJobMetricDescription& lhs, const TCustomJobMetricDescription& rhs)
{
    if (lhs.StatisticsPath != rhs.StatisticsPath) {
        return lhs.StatisticsPath < rhs.StatisticsPath;
    }
    return lhs.ProfilingName < rhs.ProfilingName;
}

void Serialize(const TCustomJobMetricDescription& customJobMetricDescription, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("statistics_path").Value(customJobMetricDescription.StatisticsPath)
            .Item("profiling_name").Value(customJobMetricDescription.ProfilingName)
            .Item("aggregate_type").Value(FormatEnum<EAggregateType>(customJobMetricDescription.AggregateType))
            .Item("job_state_filter").Value(customJobMetricDescription.JobStateFilter)
        .EndMap();
}

void Deserialize(TCustomJobMetricDescription& customJobMetricDescription, NYTree::INodePtr node)
{
    auto mapNode = node->AsMap();
    customJobMetricDescription.StatisticsPath = mapNode->GetChild("statistics_path")->GetValue<TString>();
    customJobMetricDescription.ProfilingName = mapNode->GetChild("profiling_name")->GetValue<TString>();

    auto aggregateTypeNode = mapNode->FindChild("aggregate_type");
    if (aggregateTypeNode) {
        customJobMetricDescription.AggregateType = ParseEnum<EAggregateType>(aggregateTypeNode->GetValue<TString>());
    }

    auto jobStateFilterNode = mapNode->FindChild("job_state_filter");
    if (jobStateFilterNode) {
        customJobMetricDescription.JobStateFilter = ConvertTo<std::optional<EJobState>>(jobStateFilterNode);
    }
}

////////////////////////////////////////////////////////////////////////////////

TJobMetrics TJobMetrics::FromJobStatistics(
    const TStatistics& statistics,
    EJobState jobState,
    const std::vector<TCustomJobMetricDescription>& customJobMetricDescriptions)
{
    TJobMetrics metrics;

    metrics.Values()[EJobMetricName::UserJobIoReads] =
        FindNumericValue(statistics, "/user_job/block_io/io_read").value_or(0);
    metrics.Values()[EJobMetricName::UserJobIoWrites] =
        FindNumericValue(statistics, "/user_job/block_io/io_write").value_or(0);
    metrics.Values()[EJobMetricName::UserJobIoTotal] =
        FindNumericValue(statistics, "/user_job/block_io/io_total").value_or(0);
    metrics.Values()[EJobMetricName::UserJobBytesRead] =
        FindNumericValue(statistics, "/user_job/block_io/bytes_read").value_or(0);
    metrics.Values()[EJobMetricName::UserJobBytesWritten] =
        FindNumericValue(statistics, "/user_job/block_io/bytes_written").value_or(0);

    metrics.Values()[EJobMetricName::TotalTime] =
        FindNumericValue(statistics, "/time/total").value_or(0);
    metrics.Values()[EJobMetricName::ExecTime] =
        FindNumericValue(statistics, "/time/exec").value_or(0);
    metrics.Values()[EJobMetricName::PrepareTime] =
        FindNumericValue(statistics, "/time/prepare").value_or(0);
    metrics.Values()[EJobMetricName::PrepareRootFSTime] =
        FindNumericValue(statistics, "/time/prepare_root_fs").value_or(0);
    metrics.Values()[EJobMetricName::ArtifactsDownloadTime] =
        FindNumericValue(statistics, "/time/artifacts_download").value_or(0);

    if (jobState == EJobState::Completed) {
        metrics.Values()[EJobMetricName::TotalTimeCompleted] =
            FindNumericValue(statistics, "/time/total").value_or(0);
    } else if (jobState == EJobState::Aborted) {
        metrics.Values()[EJobMetricName::TotalTimeAborted] =
            FindNumericValue(statistics, "/time/total").value_or(0);
    }

    metrics.Values()[EJobMetricName::TotalTimeOperationCompleted] = 0;
    metrics.Values()[EJobMetricName::TotalTimeOperationAborted] = 0;
    metrics.Values()[EJobMetricName::TotalTimeOperationFailed] = 0;

    metrics.Values()[EJobMetricName::AggregatedSmoothedCpuUsageX100] =
        FindNumericValue(statistics, "/job_proxy/aggregated_smoothed_cpu_usage_x100").value_or(0);
    metrics.Values()[EJobMetricName::AggregatedMaxCpuUsageX100] =
        FindNumericValue(statistics, "/job_proxy/aggregated_max_cpu_usage_x100").value_or(0);
    metrics.Values()[EJobMetricName::AggregatedPreemptableCpuX100] =
        FindNumericValue(statistics, "/job_proxy/aggregated_preemptable_cpu_x100").value_or(0);
    metrics.Values()[EJobMetricName::AggregatedPreemptedCpuX100] =
        FindNumericValue(statistics, "/job_proxy/aggregated_preempted_cpu_x100").value_or(0);

    for (const auto& jobMetricDescription : customJobMetricDescriptions) {
        if (jobMetricDescription.JobStateFilter && *jobMetricDescription.JobStateFilter != jobState) {
            continue;
        }
        i64 value = 0;
        auto summary = FindSummary(statistics, jobMetricDescription.StatisticsPath);
        if (summary) {
            switch (jobMetricDescription.AggregateType) {
                case EAggregateType::Sum:
                    value = summary->GetSum();
                    break;
                case EAggregateType::Max:
                    value = summary->GetMax();
                    break;
                case EAggregateType::Min:
                    value = summary->GetMin();
                    break;
                default:
                    YT_ABORT();
            }
        }
        metrics.CustomValues()[jobMetricDescription] = value;
    }

    return metrics;
}

bool TJobMetrics::IsEmpty() const
{
    return std::all_of(Values_.begin(), Values_.end(), [] (i64 value) { return value == 0; }) &&
        std::all_of(CustomValues_.begin(), CustomValues_.end(), [] (const auto& pair) { return pair.second == 0; });
}

void TJobMetrics::Profile(
    TMetricsAccumulator& accumulator,
    const TString& prefix,
    const NProfiling::TTagIdList& tagIds) const
{
    // NB(renadeen): you cannot use EMetricType::Gauge here.
    for (auto metricName : TEnumTraits<EJobMetricName>::GetDomainValues()) {
        auto profilingName = prefix + "/" + FormatEnum(metricName);
        accumulator.Add(profilingName, Values_[metricName], EMetricType::Counter, tagIds);
    }
    for (const auto& [jobMetriDescription, value] : CustomValues_) {
        auto profilingName = prefix + "/" + jobMetriDescription.ProfilingName;
        accumulator.Add(profilingName, value, EMetricType::Counter, tagIds);
    }
}

void TJobMetrics::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Values_);
    Persist(context, CustomValues_);
}

////////////////////////////////////////////////////////////////////////////////

TJobMetrics& operator+=(TJobMetrics& lhs, const TJobMetrics& rhs)
{
    std::transform(lhs.Values_.begin(), lhs.Values_.end(), rhs.Values_.begin(), lhs.Values_.begin(), std::plus<i64>());
    for (const auto& [jobMetriDescription, value] : rhs.CustomValues_) {
        lhs.CustomValues()[jobMetriDescription] += value;
    }
    return lhs;
}

TJobMetrics& operator-=(TJobMetrics& lhs, const TJobMetrics& rhs)
{
    std::transform(lhs.Values_.begin(), lhs.Values_.end(), rhs.Values_.begin(), lhs.Values_.begin(), std::minus<i64>());
    for (const auto& [jobMetriDescription, value] : rhs.CustomValues_) {
        lhs.CustomValues()[jobMetriDescription] -= value;
    }
    return lhs;
}

TJobMetrics operator-(const TJobMetrics& lhs, const TJobMetrics& rhs)
{
    TJobMetrics result = lhs;
    result -= rhs;
    return result;
}

TJobMetrics operator+(const TJobMetrics& lhs, const TJobMetrics& rhs)
{
    TJobMetrics result = lhs;
    result += rhs;
    return result;
}

bool Dominates(const TJobMetrics& lhs, const TJobMetrics& rhs)
{
    for (auto metric : TEnumTraits<EJobMetricName>::GetDomainValues()) {
        if (lhs.Values()[metric] < rhs.Values()[metric]) {
            return false;
        }
    }

    for (const auto& [jobMetricDescription, value] : rhs.CustomValues()) {
        auto it = lhs.CustomValues().find(jobMetricDescription);
        if (it == lhs.CustomValues().end() || it->second < value) {
            return false;
        }
    }

    return true;
}

void ToProto(NControllerAgent::NProto::TJobMetrics* protoJobMetrics, const NScheduler::TJobMetrics& jobMetrics)
{
    ToProto(protoJobMetrics->mutable_values(), jobMetrics.Values());

    // TODO(ignat): replace with proto map.
    for (const auto& [jobMetriDescription, value] : jobMetrics.CustomValues()) {
        auto* customValueProto = protoJobMetrics->add_custom_values();
        customValueProto->set_statistics_path(jobMetriDescription.StatisticsPath);
        customValueProto->set_profiling_name(jobMetriDescription.ProfilingName);
        customValueProto->set_value(value);
    }
}

void FromProto(NScheduler::TJobMetrics* jobMetrics, const NControllerAgent::NProto::TJobMetrics& protoJobMetrics)
{
    FromProto(&jobMetrics->Values(), protoJobMetrics.values());

    // TODO(ignat): replace with proto map.
    for (const auto& customValueProto : protoJobMetrics.custom_values()) {
        TCustomJobMetricDescription customJobMetric{customValueProto.statistics_path(), customValueProto.profiling_name()};
        (*jobMetrics).CustomValues().emplace(std::move(customJobMetric), customValueProto.value());
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NControllerAgent::NProto::TTreeTaggedJobMetrics* protoJobMetrics,
    const NScheduler::TTreeTaggedJobMetrics& jobMetrics)
{
    protoJobMetrics->set_tree_id(jobMetrics.TreeId);
    ToProto(protoJobMetrics->mutable_metrics(), jobMetrics.Metrics);
}

void FromProto(
    NScheduler::TTreeTaggedJobMetrics* jobMetrics,
    const NControllerAgent::NProto::TTreeTaggedJobMetrics& protoJobMetrics)
{
    jobMetrics->TreeId = protoJobMetrics.tree_id();
    FromProto(&jobMetrics->Metrics, protoJobMetrics.metrics());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

////////////////////////////////////////////////////////////////////////////////

size_t THash<NYT::NScheduler::TCustomJobMetricDescription>::operator()(const NYT::NScheduler::TCustomJobMetricDescription& jobMetricDescription) const
{
    return THash<TString>()(jobMetricDescription.StatisticsPath) * 71 + THash<TString>()(jobMetricDescription.ProfilingName);
}

////////////////////////////////////////////////////////////////////////////////
