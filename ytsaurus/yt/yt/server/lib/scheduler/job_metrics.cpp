#include "job_metrics.h"

#include <yt/yt/server/lib/controller_agent/serialize.h>

#include <yt/yt/server/lib/job_agent/structs.h>

#include <yt/yt/ytlib/controller_agent/proto/controller_agent_service.pb.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/statistics.h>

#include <util/generic/cast.h>

namespace NYT::NScheduler {

using namespace NProfiling;
using namespace NJobAgent;
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
    customJobMetricDescription.StatisticsPath = mapNode->GetChildValueOrThrow<TString>("statistics_path");
    customJobMetricDescription.ProfilingName = mapNode->GetChildValueOrThrow<TString>("profiling_name");

    auto summaryValueTypeNode = mapNode->FindChild("summary_value_type");
    if (!summaryValueTypeNode) {
        // COMPAT(ignat)
        summaryValueTypeNode = mapNode->FindChild("aggregate_type");
    }
    if (summaryValueTypeNode) {
        customJobMetricDescription.SummaryValueType = summaryValueTypeNode->GetValue<ESummaryValueType>();
    }

    auto jobStateFilterNode = mapNode->FindChild("job_state_filter");
    if (jobStateFilterNode) {
        customJobMetricDescription.JobStateFilter = jobStateFilterNode->GetValue<std::optional<EJobState>>();
    }
}

void Deserialize(TCustomJobMetricDescription& filter, NYson::TYsonPullParserCursor* cursor)
{
    Deserialize(filter, ExtractTo<INodePtr>(cursor));
}

////////////////////////////////////////////////////////////////////////////////

TJobMetrics TJobMetrics::FromJobStatistics(
    const TStatistics& jobStatistics,
    const TStatistics& controllerStatistics,
    const NJobAgent::TTimeStatistics& timeStatistics,
    EJobState jobState,
    const std::vector<TCustomJobMetricDescription>& customJobMetricDescriptions,
    bool considerNonMonotonicMetrics)
{
    TJobMetrics metrics;
    auto& metricValues = metrics.Values();
    std::fill(metricValues.begin(), metricValues.end(), 0);

    auto setMetricFromStatistics = [&] (EJobMetricName metric, const TStatistics& statistics, const TString& path) {
        metricValues[metric] = FindNumericValue(statistics, path).value_or(0);
    };

    static std::vector<std::pair<EJobMetricName, TString>> BuiltinJobMetricMapping = {
        {EJobMetricName::UserJobIoReads, "/user_job/block_io/io_read"},
        {EJobMetricName::UserJobIoWrites, "/user_job/block_io/io_write"},
        {EJobMetricName::UserJobIoTotal, "/user_job/block_io/io_total"},
        {EJobMetricName::UserJobBytesRead, "/user_job/block_io/bytes_read"},
        {EJobMetricName::UserJobBytesWritten, "/user_job/block_io/bytes_written"},
        {EJobMetricName::AggregatedSmoothedCpuUsageX100, "/job_proxy/aggregated_smoothed_cpu_usage_x100"},
        {EJobMetricName::AggregatedMaxCpuUsageX100, "/job_proxy/aggregated_max_cpu_usage_x100"},
        {EJobMetricName::AggregatedPreemptibleCpuX100, "/job_proxy/aggregated_preemptible_cpu_x100"},
        {EJobMetricName::AggregatedPreemptedCpuX100, "/job_proxy/aggregated_preempted_cpu_x100"},
    };

    for (const auto& [metric, path] : BuiltinJobMetricMapping) {
        setMetricFromStatistics(metric, jobStatistics, path);
    }

    setMetricFromStatistics(EJobMetricName::TotalTime, controllerStatistics, "/time/total");

    static std::vector BuiltinControllerTimeMetricMapping{
        std::pair{EJobMetricName::ExecTime, &TTimeStatistics::ExecDuration},
        std::pair{EJobMetricName::PrepareTime, &TTimeStatistics::PrepareDuration},
        std::pair{EJobMetricName::PrepareRootFSTime, &TTimeStatistics::PrepareRootFSDuration},
        std::pair{EJobMetricName::ArtifactsDownloadTime, &TTimeStatistics::ArtifactsDownloadDuration},
    };

    for (const auto& [metric, field] : BuiltinControllerTimeMetricMapping) {
        metricValues[metric] = (timeStatistics.*field).value_or(TDuration{}).MilliSeconds();
    }

    if (jobState == EJobState::Completed) {
        setMetricFromStatistics(EJobMetricName::TotalTimeCompleted, controllerStatistics, "/time/total");
    } else if (jobState == EJobState::Aborted) {
        setMetricFromStatistics(EJobMetricName::TotalTimeAborted, controllerStatistics, "/time/total");
    }

    for (const auto& jobMetricDescription : customJobMetricDescriptions) {
        if (jobMetricDescription.JobStateFilter && *jobMetricDescription.JobStateFilter != jobState) {
            continue;
        }
        i64 value = 0;
        auto summary = FindSummary(jobStatistics, jobMetricDescription.StatisticsPath);
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
    for (const auto& [jobMetricDescription, value] : rhs.CustomValues_) {
        lhs.CustomValues()[jobMetricDescription] += value;
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
    return THash<TString>()(jobMetricDescription.ProfilingName);
}

////////////////////////////////////////////////////////////////////////////////
