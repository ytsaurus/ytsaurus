#include "describe_computation.h"
#include "describe_pipeline.h"
#include "fill_graph_limits.h"
#include "intermediate_description.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/job_directory.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

namespace NYT::NFlow::NDescribe {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

void TPerformanceMetricsWithExamplePartitions::Register(TRegistrar registrar)
{
    registrar.Parameter("cpu_usage_example_partition", &TThis::CpuUsageExamplePartition)
        .Default();
    registrar.Parameter("memory_usage_example_partition", &TThis::MemoryUsageExamplePartition)
        .Default();
    registrar.Parameter("messages_per_second_example_partition", &TThis::MessagesPerSecondExamplePartition)
        .Default();
    registrar.Parameter("bytes_per_second_example_partition", &TThis::BytesPerSecondExamplePartition)
        .Default();
}

void TComputationMultidimensionPerformanceMetrics::Register(TRegistrar registrar)
{
    registrar.Parameter("total", &TThis::Total)
        .Default();
    registrar.Parameter("average", &TThis::Average)
        .Default();
    registrar.Parameter("max", &TThis::Max)
        .Default();
    registrar.Parameter("min", &TThis::Min)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TComputationPartitionCountWithExample::Register(TRegistrar registrar)
{
    registrar.Parameter("count", &TThis::Count)
        .Default(0);
    registrar.Parameter("example_partition", &TThis::ExamplePartition)
        .Default();
}

void TComputationPartitionErrorsByReason::Register(TRegistrar registrar)
{
    registrar.Parameter("restart_because_fail", &TThis::RestartBecauseFail)
        .Default();
    registrar.Parameter("restart_because_rebalancing", &TThis::RestartBecauseRebalancing)
        .Default();
    registrar.Parameter("has_retryable_errors", &TThis::HasRetryableError)
        .Default();
    registrar.Parameter("total_with_problems", &TThis::TotalWithProblems)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TExtendedComputationDescription::Register(TRegistrar registrar)
{
    registrar.Parameter("performance_metrics", &TThis::PerformanceMetrics)
        .Default();
    registrar.Parameter("partitions", &TThis::Partitions)
        .Default();
    registrar.Parameter("partition_with_error_by_time_and_type", &TThis::PartitionWithErrorByTimeAndType)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void FillComputationPerformanceMetrics(TExtendedComputationDescription& description)
{
    if (description.Partitions.empty()) {
        return;
    }


    auto doForAllFields = [] (auto callback) {
#define XX(metric) callback(                     \
    [] (auto& metrics) -> auto& {                \
        return metrics.metric;                   \
    },                                           \
    [] (auto& metrics) -> auto& {                \
        return metrics.metric##ExamplePartition; \
    });
        YTFLOW_ITERATE_PERFORMANCE_METRICS_FIELDS(XX);
#undef XX
    };


    auto getClosestPartition = [&] (auto getter, auto& valueWithField) {
        return MinElementBy(
            description.Partitions,
            [&] (const auto& partition) {
                return std::abs(getter(partition) - getter(valueWithField));
            })
            ->PartitionId;
    };

    // Total & average.
    doForAllFields([&] (auto getter, auto examplePartitionGetter) {
        getter(description.PerformanceMetrics.Total) = 0;
        for (const auto& partition : description.Partitions) {
            getter(description.PerformanceMetrics.Total) += getter(partition);
        }
        getter(description.PerformanceMetrics.Average) =
            getter(description.PerformanceMetrics.Total) / description.Partitions.size();
        examplePartitionGetter(description.PerformanceMetrics.Average) =
            getClosestPartition(getter, description.PerformanceMetrics.Average);
    });

    // Min.
    doForAllFields([&] (auto getter, auto examplePartitionGetter) {
        getter(description.PerformanceMetrics.Min) = getter(description.Partitions.front());
        for (const auto& partition : description.Partitions) {
            getter(description.PerformanceMetrics.Min) = std::min(
                getter(description.PerformanceMetrics.Min),
                getter(partition));
        }
        examplePartitionGetter(description.PerformanceMetrics.Min) =
            getClosestPartition(getter, description.PerformanceMetrics.Min);
    });

    // Max.
    doForAllFields([&] (auto getter, auto examplePartitionGetter) {
        getter(description.PerformanceMetrics.Max) = getter(description.Partitions.front());
        for (const auto& partition : description.Partitions) {
            getter(description.PerformanceMetrics.Max) = std::max(
                getter(description.PerformanceMetrics.Max),
                getter(partition));
        }
        examplePartitionGetter(description.PerformanceMetrics.Max) =
            getClosestPartition(getter, description.PerformanceMetrics.Max);
    });
}

////////////////////////////////////////////////////////////////////////////////

void FillComputationPartitionErrorMetrics(TExtendedComputationDescription& description, TInstant now)
{
    for (std::string periodStr : {"1m", "5m", "30m"}) {
        auto threshold = now - TDuration::Parse(periodStr);
        auto& metricsByTime = description.PartitionWithErrorByTimeAndType[periodStr];

        // TODO: Write tests.
        for (auto& partition : description.Partitions) {
            auto onProblem = [&, problemOccured = false] (auto& countWithExample) mutable {
                countWithExample.Count += 1;
                countWithExample.ExamplePartition = partition.PartitionId;

                if (problemOccured) {
                    return;
                }
                problemOccured = true;
                metricsByTime.TotalWithProblems.Count += 1;
                metricsByTime.TotalWithProblems.ExamplePartition = partition.PartitionId;
            };

            if (partition.PreviousJobFailInstant > threshold) {
                onProblem(metricsByTime.RestartBecauseFail);
            }
            if (partition.PreviousRebalancingInstant > threshold) {
                onProblem(metricsByTime.RestartBecauseRebalancing);
            }
            if (partition.LastRetryableErrorInstant > threshold) {
                onProblem(metricsByTime.HasRetryableError);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void FillPerformanceMessage(
    TExtendedComputationDescription& description,
    const std::vector<TPartitionIntermediateDescription>& partitions)
{
    using TEptPart = std::string;
    using TLimitType = std::string;

    auto& message = description.Messages.emplace_back();
    message.Level = ELogLevel::Info;
    message.Text = "Performance info";

    TStringBuilder markdown;

    auto computeUsage = [] (i64 used, i64 limit) -> double {
        return static_cast<double>(used) / std::max<i64>(1, limit);
    };

    auto formatUsagePercentage = [] (double usage) -> std::string {
        return usage > 0.5 ? Format("**%.1f%%**", usage * 100) : Format("%.1f%%", usage * 100);
    };

    // Helper function to format a value with 3 decimal places, bolding if > 0.5.
    auto formatValue = [] (double value) -> std::string {
        return value > 0.5 ? Format("**%.3f**", value) : Format("%.3f", value);
    };

    // Process epoch part times.
    THashMap<TEptPart, double> totalEptSum;
    THashMap<TEptPart, std::pair<TPartitionId, double>> maxEptByPart;

    for (const auto& partitionDesc : partitions) {
        if (!partitionDesc.Partition) {
            continue;
        }
        auto partitionId = partitionDesc.Partition->PartitionId;

        if (partitionDesc.PartitionJobStatus && partitionDesc.PartitionJobStatus->CurrentJobStatus) {
            const auto& ept = partitionDesc.PartitionJobStatus->CurrentJobStatus->EpochPartTimes;
            if (!ept.empty()) {
                double sum = 0.0;
                for (const auto& [part, value] : ept) {
                    sum += value;
                }

                if (sum > 0) {
                    for (const auto& [part, value] : ept) {
                        double normalizedValue = value / sum;

                        totalEptSum[part] += normalizedValue;

                        auto& maxEntry = maxEptByPart[part];
                        if (normalizedValue > maxEntry.second) {
                            maxEntry = {partitionId, normalizedValue};
                        }
                    }
                }
            }
        }
    }

    // Table 1: Sum of EPT across all partitions (normalized to sum = 1).
    if (!totalEptSum.empty()) {
        // Normalize total sum to 1.
        double grandTotal = 0.0;
        for (const auto& [part, sum] : totalEptSum) {
            grandTotal += sum;
        }

        markdown.AppendString("## Epoch Part Times (Sum)\n\n");
        markdown.AppendString("| Component | Sum | Histogram |\n");
        markdown.AppendString("|-----------|-----|----------|\n");

        std::vector<std::pair<TEptPart, double>> sumEpt;
        for (const auto& [part, sum] : totalEptSum) {
            double normalizedSum = grandTotal > 0 ? sum / grandTotal : 0.0;
            sumEpt.emplace_back(part, normalizedSum);
        }

        SortBy(sumEpt, [] (const auto& item) {
            return -item.second; // Negative for descending order.
        });

        constexpr int MaxHistogramWidth = 20;
        for (const auto& [part, sum] : sumEpt) {
            markdown.AppendFormat("| %v | %v | %v |\n", part, formatValue(sum), std::string(static_cast<int>(sum * MaxHistogramWidth), '#'));
        }
        markdown.AppendString("\n");
    }

    // Table 2: Maximum EPT across all partitions.
    if (!maxEptByPart.empty()) {
        markdown.AppendString("## Epoch Part Times (Max)\n\n");
        markdown.AppendString("| Component | Max | Example partition |\n");
        markdown.AppendString("|-----------|-----|-------------------|\n");

        std::vector<std::tuple<TEptPart, double, TPartitionId>> maxEpt;
        for (const auto& [key, maxEntry] : maxEptByPart) {
            maxEpt.emplace_back(key, maxEntry.second, maxEntry.first);
        }

        SortBy(maxEpt, [] (const auto& item) {
            return -std::get<1>(item);
        });

        for (const auto& [key, max, partitionId] : maxEpt) {
            TString valueStr = formatValue(max);
            markdown.AppendFormat("| %v | %v | %v |\n", key, valueStr, partitionId);
        }
        markdown.AppendString("\n");
    }

    // Helper structure to store partition limit data.
    struct TPartitionLimitData
    {
        TPartitionId PartitionId;
        TJobEntityLimitStatus LimitStatus;
    };

    // Helper function to collect and process limits (both input and output).
    auto collectAndProcessLimits = [&] (
        const std::function<const THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>>&(const TPartitionIntermediateDescription&)>& limitsGetter,
        const TString& sectionName) {
        THashMap<TLimitType, std::vector<TPartitionLimitData>> limitsByType;

        // Collect limits by type from all partitions.
        for (const auto& partitionDesc : partitions) {
            if (!partitionDesc.Partition) {
                continue;
            }

            if (partitionDesc.PartitionJobStatus && partitionDesc.PartitionJobStatus->CurrentJobStatus) {
                const auto& limits = limitsGetter(partitionDesc);
                for (const auto& [limitType, streamLimits] : limits) {
                    for (const auto& [streamId, limitStatus] : streamLimits) {
                        limitsByType[limitType].push_back({partitionDesc.Partition->PartitionId, limitStatus});
                    }
                }
            }
        }

        if (limitsByType.empty()) {
            return;
        }

        auto sortedLimitTypes = GetKeys(limitsByType);
        Sort(sortedLimitTypes);

        // Summary table.
        markdown.AppendFormat("## %v Limits (Summary)\n\n", sectionName);
        markdown.AppendString("| Limit Type | Usage % | Used | Pending | Limit |\n");
        markdown.AppendString("|------------|---------|------|---------|-------|\n");

        for (const auto& limitType : sortedLimitTypes) {
            i64 totalUsed = 0;
            i64 totalPending = 0;
            i64 totalLimit = 0;

            for (const auto& data : limitsByType[limitType]) {
                totalUsed += data.LimitStatus.Used;
                totalPending += data.LimitStatus.Pending.value_or(0);
                totalLimit += data.LimitStatus.Limit;
            }

            TString usageStr = formatUsagePercentage(computeUsage(totalUsed, totalLimit));
            markdown.AppendFormat("| %v | %v | %v | %v | %v |\n", limitType, usageStr, totalUsed, totalPending, totalLimit);
        }
        markdown.AppendString("\n");

        // Max table.
        markdown.AppendFormat("## %v Limits (Max by Partition)\n\n", sectionName);
        markdown.AppendString("| Limit Type | Usage % | Partition achieving maximum | Used | Pending | Limit |\n");
        markdown.AppendString("|------------|---------|-----------------------------|------|---------|-------|\n");

        for (const auto& limitType : sortedLimitTypes) {
            YT_VERIFY(!limitsByType[limitType].empty());
            const auto& data = *MaxElementBy(
                limitsByType[limitType],
                [&computeUsage] (const TPartitionLimitData& data) -> double {
                    return computeUsage(data.LimitStatus.Used, data.LimitStatus.Limit);
                });
            markdown.AppendFormat("| %v | %v | %v | %v | %v | %v |\n",
                limitType,
                formatUsagePercentage(computeUsage(data.LimitStatus.Used, data.LimitStatus.Limit)),
                data.PartitionId,
                data.LimitStatus.Used,
                data.LimitStatus.Pending.value_or(0),
                data.LimitStatus.Limit);
        }
        markdown.AppendString("\n");
    };

    // Process input and output limits using the helper function.
    collectAndProcessLimits(
        [] (const TPartitionIntermediateDescription& desc) -> const auto& {
            return desc.PartitionJobStatus->CurrentJobStatus->InputLimits;
        },
        "Input");

    collectAndProcessLimits(
        [] (const TPartitionIntermediateDescription& desc) -> const auto& {
            return desc.PartitionJobStatus->CurrentJobStatus->OutputLimits;
        },
        "Output");

    message.MarkdownText = markdown.Flush();
}

////////////////////////////////////////////////////////////////////////////////

TExtendedComputationDescription DescribeComputation(
    const TFlowViewPtr& flowView,
    const TComputationId& computationId)
{
    // Every computation's partitions, not just this one's: the writer-blocked
    // shares below come from the neighbors. One walk of the layout serves both.
    auto intermediateDescriptions = GetComputationPartitionIntermediateDescriptions(flowView);
    THashMap<TComputationId, std::vector<TPartitionIntermediateDescription>> computationIntermediateDescriptions{
        {computationId, GetOrDefault(intermediateDescriptions, computationId)}};
    auto computationDescriptions = MakeComputationDescriptions(flowView, computationIntermediateDescriptions); // Do some useless work, but it is not too heavy.
    auto computationDescriptionIt = computationDescriptions.find(computationId);
    THROW_ERROR_EXCEPTION_IF(computationDescriptionIt == computationDescriptions.end(), "Computation %Qv not found", computationId);
    auto description = ConvertTo<TExtendedComputationDescription>(computationDescriptionIt->second);
    auto partitions = GetOrDefault(computationIntermediateDescriptions, computationId);

    for (const auto& partitionDesc : partitions) {
        std::vector<TMessage> messages; // To be ignored.
        FillPartitionDescription(partitionDesc, description.Partitions.emplace_back(), messages);
    }
    FillComputationPerformanceMetrics(description);
    FillComputationPartitionErrorMetrics(description, TInstant::Seconds(flowView->State->CurrentTimestamp.Underlying()));
    FillTopHeavyHittersMessages(flowView, computationIntermediateDescriptions, computationId, description);

    if (auto* state = flowView->State->JobManagerState->Computations.FindPtr(computationId)) {
        auto& message = description.Messages.emplace_back();
        message.Level = ELogLevel::Info;
        message.Text = "Computation controller (job manager) state";
        message.Yson = ConvertToYsonString(*state, NYson::EYsonFormat::Text);
    }

    FillPerformanceMessage(description, partitions);

    // The renderer needs a pipeline description filled by RegisterStreams and
    // FillGraphLimits; every computation is registered because the writer-blocked
    // shares come from the neighbors.
    {
        const auto& pipelineSpec = flowView->State->ExecutionSpec->PipelineSpec->GetValue();
        TDescribeTraitsContext describeTraitsContext{.PipelinePath = flowView->EphemeralState->PipelinePath};
        TPipelineDescription bufferPipeline;
        for (const auto& [otherComputationId, otherComputationSpec] : pipelineSpec->Computations) {
            RegisterStreams(pipelineSpec, otherComputationId, bufferPipeline, bufferPipeline.Computations[otherComputationId], describeTraitsContext);
        }
        FillGraphLimits(flowView, intermediateDescriptions, bufferPipeline);
        if (auto message = BuildBuffersAndBackpressureMessage(
            bufferPipeline.Computations[computationId],
            ComputeWriterBlockedTimeShares(bufferPipeline)))
        {
            description.Messages.push_back(std::move(*message));
        }
    }

    return description;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
