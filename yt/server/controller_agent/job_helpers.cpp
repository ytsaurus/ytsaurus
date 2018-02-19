#include "job_helpers.h"
#include "config.h"

#include <yt/server/chunk_pools/chunk_pool.h>

#include <yt/server/scheduler/config.h>
#include <yt/server/scheduler/job.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NControllerAgent {

using namespace NChunkPools;
using namespace NYson;
using namespace NYTree;
using namespace NYPath;
using namespace NChunkClient;
using namespace NPhoenix;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

static const TString InputRowCountPath = "/data/input/row_count";
static const TString InputUncompressedDataSizePath = "/data/input/uncompressed_data_size";
static const TString InputCompressedDataSizePath = "/data/input/compressed_data_size";
static const TString InputDataWeightPath = "/data/input/data_weight";
static const TString InputPipeIdleTimePath = "/user_job/pipes/input/idle_time";
static const TString JobProxyCpuUsagePath = "/job_proxy/cpu/user";

////////////////////////////////////////////////////////////////////////////////

void TBriefJobStatistics::Persist(const NPhoenix::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Timestamp);
    Persist(context, ProcessedInputRowCount);
    Persist(context, ProcessedInputUncompressedDataSize);
    Persist(context, ProcessedInputCompressedDataSize);
    Persist(context, ProcessedInputDataWeight);
    Persist(context, ProcessedOutputRowCount);
    Persist(context, ProcessedOutputUncompressedDataSize);
    Persist(context, ProcessedOutputCompressedDataSize);
    Persist(context, InputPipeIdleTime);
    Persist(context, OutputPipeIdleTime);
    Persist(context, JobProxyCpuUsage);
}

void Serialize(const TBriefJobStatisticsPtr& briefJobStatistics, IYsonConsumer* consumer)
{
    if (!briefJobStatistics) {
        BuildYsonFluently(consumer)
            .BeginMap()
            .EndMap();
        return;
    }

    BuildYsonFluently(consumer)
        .BeginAttributes()
            .Item("timestamp").Value(briefJobStatistics->Timestamp)
        .EndAttributes()
        .BeginMap()
            .Item("processed_input_row_count").Value(briefJobStatistics->ProcessedInputRowCount)
            .Item("processed_input_uncompressed_data_size").Value(briefJobStatistics->ProcessedInputUncompressedDataSize)
            .Item("processed_input_compressed_data_size").Value(briefJobStatistics->ProcessedInputCompressedDataSize)
            .Item("processed_input_data_weight").Value(briefJobStatistics->ProcessedInputDataWeight)
            .Item("processed_output_uncompressed_data_size").Value(briefJobStatistics->ProcessedOutputUncompressedDataSize)
            .Item("processed_output_compressed_data_size").Value(briefJobStatistics->ProcessedOutputCompressedDataSize)
            .DoIf(static_cast<bool>(briefJobStatistics->InputPipeIdleTime), [&] (TFluentMap fluent) {
                fluent.Item("input_pipe_idle_time").Value(*(briefJobStatistics->InputPipeIdleTime));
            })
            .DoIf(static_cast<bool>(briefJobStatistics->OutputPipeIdleTime), [&] (TFluentMap fluent) {
                fluent.Item("output_pipe_idle_time").Value(*(briefJobStatistics->OutputPipeIdleTime));
            })
            .DoIf(static_cast<bool>(briefJobStatistics->JobProxyCpuUsage), [&] (TFluentMap fluent) {
                fluent.Item("job_proxy_cpu_usage").Value(*(briefJobStatistics->JobProxyCpuUsage));
            })
        .EndMap();
}

TString ToString(const TBriefJobStatisticsPtr& briefStatistics)
{
    return Format("{PIRC: %v, PIUDS: %v, PIDW: %v, PICDS: %v, PORC: %v, POUDS: %v, POCDS: %v, IPIT: %v, OPIT: %v, JPCU: %v, T: %v}",
        briefStatistics->ProcessedInputRowCount,
        briefStatistics->ProcessedInputUncompressedDataSize,
        briefStatistics->ProcessedInputDataWeight,
        briefStatistics->ProcessedInputCompressedDataSize,
        briefStatistics->ProcessedOutputRowCount,
        briefStatistics->ProcessedOutputUncompressedDataSize,
        briefStatistics->ProcessedOutputCompressedDataSize,
        briefStatistics->InputPipeIdleTime,
        briefStatistics->OutputPipeIdleTime,
        briefStatistics->JobProxyCpuUsage,
        briefStatistics->Timestamp);
}

////////////////////////////////////////////////////////////////////////////////

bool CheckJobActivity(
    const TBriefJobStatisticsPtr& lhs,
    const TBriefJobStatisticsPtr& rhs,
    const TSuspiciousJobsOptionsPtr& options)
{
    bool wasActive = lhs->ProcessedInputRowCount < rhs->ProcessedInputRowCount;
    wasActive |= lhs->ProcessedInputUncompressedDataSize < rhs->ProcessedInputUncompressedDataSize;
    wasActive |= lhs->ProcessedInputCompressedDataSize < rhs->ProcessedInputCompressedDataSize;
    wasActive |= lhs->ProcessedOutputRowCount < rhs->ProcessedOutputRowCount;
    wasActive |= lhs->ProcessedOutputUncompressedDataSize < rhs->ProcessedOutputUncompressedDataSize;
    wasActive |= lhs->ProcessedOutputCompressedDataSize < rhs->ProcessedOutputCompressedDataSize;
    if (lhs->JobProxyCpuUsage && rhs->JobProxyCpuUsage) {
        wasActive |= *lhs->JobProxyCpuUsage + options->CpuUsageThreshold < *rhs->JobProxyCpuUsage;
    }
    if (lhs->InputPipeIdleTime && rhs->InputPipeIdleTime && lhs->Timestamp < rhs->Timestamp) {
        wasActive |= (*rhs->InputPipeIdleTime - *lhs->InputPipeIdleTime) <
            (rhs->Timestamp - lhs->Timestamp).MilliSeconds() * options->InputPipeIdleTimeFraction;
    }
    if (lhs->OutputPipeIdleTime && rhs->OutputPipeIdleTime && lhs->Timestamp < rhs->Timestamp) {
        wasActive |= (*rhs->OutputPipeIdleTime - *lhs->OutputPipeIdleTime) <
            (rhs->Timestamp - lhs->Timestamp).MilliSeconds() * options->OutputPipeIdleTimeFraction;
    }
    return wasActive;
}

TBriefJobStatisticsPtr BuildBriefStatistics(std::unique_ptr<TJobSummary> jobSummary)
{
    YCHECK(jobSummary->Statistics);
    const auto& statistics = *jobSummary->Statistics;

    auto briefStatistics = New<TBriefJobStatistics>();
    briefStatistics->ProcessedInputRowCount = GetNumericValue(statistics, InputRowCountPath);
    briefStatistics->ProcessedInputUncompressedDataSize = GetNumericValue(statistics, InputUncompressedDataSizePath);
    briefStatistics->ProcessedInputCompressedDataSize = GetNumericValue(statistics, InputCompressedDataSizePath);
    briefStatistics->ProcessedInputDataWeight = GetNumericValue(statistics, InputDataWeightPath);
    briefStatistics->InputPipeIdleTime = FindNumericValue(statistics, InputPipeIdleTimePath);
    briefStatistics->JobProxyCpuUsage = FindNumericValue(statistics, JobProxyCpuUsagePath);
    briefStatistics->Timestamp = statistics.GetTimestamp().Get(TInstant::Now());

    auto outputPipeIdleTimes = GetOutputPipeIdleTimes(statistics);
    if (!outputPipeIdleTimes.empty()) {
        briefStatistics->OutputPipeIdleTime = 0;
        // This is a simplest way to achieve the desired result, although not the most fair one.
        for (const auto& pair : outputPipeIdleTimes) {
            briefStatistics->OutputPipeIdleTime = std::max<i64>(*briefStatistics->OutputPipeIdleTime, pair.second);
        }
    }

    // TODO(max42): GetTotalOutputDataStatistics is implemented very inefficiently (it creates yhash containing
    // output data statistics per output table and then aggregates them). Rewrite it without any new allocations.
    auto outputDataStatistics = GetTotalOutputDataStatistics(statistics);
    briefStatistics->ProcessedOutputUncompressedDataSize = outputDataStatistics.uncompressed_data_size();
    briefStatistics->ProcessedOutputCompressedDataSize = outputDataStatistics.compressed_data_size();
    briefStatistics->ProcessedOutputRowCount = outputDataStatistics.row_count();

    return briefStatistics;
}

void ParseStatistics(TJobSummary* jobSummary, const TYsonString& lastObservedStatisticsYson)
{
    if (!jobSummary->StatisticsYson) {
        jobSummary->StatisticsYson = lastObservedStatisticsYson;
    }

    auto& statistics = jobSummary->Statistics;
    const auto& statisticsYson = jobSummary->StatisticsYson;
    if (statisticsYson) {
        statistics = ConvertTo<NJobTrackerClient::TStatistics>(statisticsYson);
        // NB: we should remove timestamp from the statistics as it becomes a YSON-attribute
        // when writing it to the event log, but top-level attributes are disallowed in table rows.
        statistics->SetTimestamp(Null);
    } else {
        statistics = NJobTrackerClient::TStatistics();
    }
}

static void BuildInputSliceLimit(
    const TInputDataSlicePtr& slice,
    const TInputSliceLimit& limit,
    TNullable<i64> rowIndex,
    TFluentAny fluent)
{
    fluent.BeginMap()
        .DoIf((limit.RowIndex.operator bool() || rowIndex) && slice->IsTrivial(), [&] (TFluentMap fluent) {
            fluent
                .Item("row_index").Value(
                    limit.RowIndex.Get(rowIndex.Get(0)) + slice->GetSingleUnversionedChunkOrThrow()->GetTableRowIndex());
        })
        .DoIf(limit.Key.operator bool(), [&] (TFluentMap fluent) {
            fluent
                .Item("key").Value(limit.Key);
        })
    .EndMap();
}

TYsonString BuildInputPaths(
    const std::vector<TRichYPath>& inputPaths,
    const TChunkStripeListPtr& inputStripeList,
    EOperationType operationType,
    EJobType jobType)
{
    bool hasSlices = false;
    std::vector<std::vector<TInputDataSlicePtr>> slicesByTable(inputPaths.size());
    for (const auto& stripe : inputStripeList->Stripes) {
        for (const auto& slice : stripe->DataSlices) {
            auto tableIndex = slice->GetTableIndex();
            if (tableIndex >= 0) {
                slicesByTable[tableIndex].push_back(slice);
                hasSlices = true;
            }
        }
    }
    if (!hasSlices) {
        return TYsonString();
    }

    std::vector<char> isForeignTable(inputPaths.size());
    std::transform(
        inputPaths.begin(),
        inputPaths.end(),
        isForeignTable.begin(),
        [](const TRichYPath& path) { return path.GetForeign(); });

    std::vector<std::vector<std::pair<TInputDataSlicePtr, TInputDataSlicePtr>>> rangesByTable(inputPaths.size());
    bool mergeByRows = !(
        operationType == EOperationType::Reduce ||
        (operationType == EOperationType::Merge && jobType == EJobType::SortedMerge));
    for (int tableIndex = 0; tableIndex < static_cast<int>(slicesByTable.size()); ++tableIndex) {
        auto& tableSlices = slicesByTable[tableIndex];

        std::sort(tableSlices.begin(), tableSlices.end(), &CompareDataSlicesByLowerLimit);

        int firstSlice = 0;
        while (firstSlice < static_cast<int>(tableSlices.size())) {
            int lastSlice = firstSlice + 1;
            while (lastSlice < static_cast<int>(tableSlices.size())) {
                if (mergeByRows && !isForeignTable[tableIndex] &&
                    !CanMergeSlices(tableSlices[lastSlice - 1], tableSlices[lastSlice]))
                {
                    break;
                }
                ++lastSlice;
            }
            rangesByTable[tableIndex].emplace_back(tableSlices[firstSlice], tableSlices[lastSlice - 1]);
            firstSlice = lastSlice;
        }
    }

    return BuildYsonStringFluently()
        .DoListFor(rangesByTable, [&] (TFluentList fluent, const std::vector<std::pair<TInputDataSlicePtr, TInputDataSlicePtr>>& tableRanges) {
            fluent
                .DoIf(!tableRanges.empty(), [&] (TFluentList fluent) {
                    int tableIndex = tableRanges[0].first->GetTableIndex();
                    fluent
                        .Item()
                        .BeginAttributes()
                            .DoIf(isForeignTable[tableIndex], [&] (TFluentMap fluent) {
                                fluent
                                    .Item("foreign").Value(true);
                            })
                            .Item("ranges")
                            .DoListFor(tableRanges, [&] (TFluentList fluent, const std::pair<TInputDataSlicePtr, TInputDataSlicePtr>& range) {
                                fluent
                                    .Item()
                                    .BeginMap()
                                        .Item("lower_limit").Do(BIND(
                                            &BuildInputSliceLimit,
                                            range.first,
                                            range.first->LowerLimit(),
                                            TNullable<i64>(mergeByRows && !isForeignTable[tableIndex], 0)))
                                        .Item("upper_limit").Do(BIND(
                                            &BuildInputSliceLimit,
                                            range.second,
                                            range.second->UpperLimit(),
                                            TNullable<i64>(mergeByRows && !isForeignTable[tableIndex], range.second->GetRowCount())))
                                    .EndMap();
                            })
                        .EndAttributes()
                        .Value(inputPaths[tableIndex].GetPath());
                });
        });
}

////////////////////////////////////////////////////////////////////////////////

void TScheduleJobStatistics::RecordJobResult(const TScheduleJobResultPtr& scheduleJobResult)
{
    for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues()) {
        Failed[reason] += scheduleJobResult->Failed[reason];
    }
    Duration += scheduleJobResult->Duration;
    ++Count;
}

void TScheduleJobStatistics::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Failed);
    Persist(context, Duration);
    Persist(context, Count);
}

DECLARE_DYNAMIC_PHOENIX_TYPE(TScheduleJobStatistics, 0x1ba9c7e0);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
