#include "job_metrics.h"

#include <yt/yt/server/lib/controller_agent/serialize.h>

#include <yt/yt/ytlib/controller_agent/proto/controller_agent_service.pb.h>

#include <yt/yt/core/profiling/profiler.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/statistics.h>

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
    Persist(context, SummaryValueType);
    Persist(context, JobStateFilter);
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
            .Item("summary_value_type").Value(FormatEnum<ESummaryValueType>(customJobMetricDescription.SummaryValueType))
            // COMPAT(ignat)
            .Item("aggregate_type").Value(FormatEnum<ESummaryValueType>(customJobMetricDescription.SummaryValueType))
            .Item("job_state_filter").Value(customJobMetricDescription.JobStateFilter)
        .EndMap();
}

void Deserialize(TCustomJobMetricDescription& customJobMetricDescription, NYTree::INodePtr node)
{
    auto mapNode = node->AsMap();
    customJobMetricDescription.StatisticsPath = mapNode->GetChildOrThrow("statistics_path")->GetValue<TString>();
    customJobMetricDescription.ProfilingName = mapNode->GetChildOrThrow("profiling_name")->GetValue<TString>();

    auto summaryValueTypeNode = mapNode->FindChild("summary_value_type");
    if (!summaryValueTypeNode) {
        // COMPAT(ignat)
        summaryValueTypeNode = mapNode->FindChild("aggregate_type");
    }
    if (summaryValueTypeNode) {
        customJobMetricDescription.SummaryValueType = ParseEnum<ESummaryValueType>(summaryValueTypeNode->GetValue<TString>());
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
    const std::vector<TCustomJobMetricDescription>& customJobMetricDescriptions,
    bool considerNonMonotonicMetrics)
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
    
    metrics.Values()[EJobMetricName::MainResourceConsumptionOperationCompleted] = 0;
    metrics.Values()[EJobMetricName::MainResourceConsumptionOperationAborted] = 0;
    metrics.Values()[EJobMetricName::MainResourceConsumptionOperationFailed] = 0;

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
            switch (jobMetricDescription.SummaryValueType) {
                case ESummaryValueType::Sum:
                    value = summary->GetSum();
                    break;
                case ESummaryValueType::Max:
                    value = summary->GetMax();
                    break;
                case ESummaryValueType::Min:
                    if (considerNonMonotonicMetrics) {
                        value = summary->GetMin();
                    }
                    break;
                case ESummaryValueType::Last:
                    if (considerNonMonotonicMetrics) {
                        value = summary->GetLast().value_or(0);
                    }
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

void TJobMetrics::Profile(NProfiling::ISensorWriter* writer) const
{
    // NB: all job metrics are counters since we use straightforward aggregation of deltas.
    for (auto metricName : TEnumTraits<EJobMetricName>::GetDomainValues()) {
        writer->AddCounter("/metrics/" + FormatEnum(metricName), Values_[metricName]);
    }
    for (const auto& [jobMetriDescription, value] : CustomValues_) {
        writer->AddCounter("/metrics/" + jobMetriDescription.ProfilingName, value);
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

TJobMetrics Max(const TJobMetrics& lhs, const TJobMetrics& rhs)
{
    TJobMetrics result;
    for (auto metricName : TEnumTraits<EJobMetricName>::GetDomainValues()) {
        result.Values()[metricName] = std::max(lhs.Values()[metricName], rhs.Values()[metricName]);
    }

    for (const auto& [jobMetriDescription, rhsValue] : rhs.CustomValues()) {
        auto lhsIt = lhs.CustomValues().find(jobMetriDescription);
        if (lhs.CustomValues().find(jobMetriDescription) == lhs.CustomValues().end()) {
            result.CustomValues()[jobMetriDescription] = rhsValue;
        } else {
            result.CustomValues()[jobMetriDescription] = std::max(lhsIt->second, rhsValue);
        }
    }
    for (const auto& [jobMetriDescription, lhsValue] : lhs.CustomValues()) {
        auto rhsIt = rhs.CustomValues().find(jobMetriDescription);
        if (rhs.CustomValues().find(jobMetriDescription) == rhs.CustomValues().end()) {
            result.CustomValues()[jobMetriDescription] = lhsValue;
        } else {
            result.CustomValues()[jobMetriDescription] = std::max(rhsIt->second, lhsValue);
        }
    }
    return result;
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

void Serialize(const TJobMetrics& jobMetrics, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginList()
            .DoFor(TEnumTraits<EJobMetricName>::GetDomainValues(), [&] (TFluentList fluent, EJobMetricName name) {
                fluent.Item()
                    .BeginMap()
                        .Item("name").Value(FormatEnum(name))
                        .Item("value").Value(jobMetrics.Values()[name])
                    .EndMap();
            })
            .DoFor(jobMetrics.CustomValues(), [&] (TFluentList fluent, const std::pair<TCustomJobMetricDescription, i64>& pair) {
                const auto& [jobMetricDescription, value] = pair;
                fluent.Item()
                    .BeginMap()
                        .Item("statistics_path").Value(jobMetricDescription.StatisticsPath)
                        .Item("value").Value(value)
                    .EndMap();
            })
        .EndList();
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
