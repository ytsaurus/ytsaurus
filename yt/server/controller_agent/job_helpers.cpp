#include "job_helpers.h"
#include "controller_agent.h"
#include "config.h"

#include <yt/server/lib/chunk_pools/chunk_pool.h>

#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NControllerAgent {

using namespace NChunkPools;
using namespace NYson;
using namespace NYTree;
using namespace NYPath;
using namespace NChunkClient;
using namespace NPhoenix;
using namespace NScheduler;
using namespace NJobTrackerClient;

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
            .OptionalItem("input_pipe_idle_time", briefJobStatistics->InputPipeIdleTime)
            .OptionalItem("output_pipe_idle_time", briefJobStatistics->OutputPipeIdleTime)
            .OptionalItem("job_proxy_cpu_usage", briefJobStatistics->JobProxyCpuUsage)
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
    const TSuspiciousJobsOptionsPtr& options,
    EJobType jobType)
{
    if (jobType == EJobType::Vanilla) {
        // It is hard to deduce that the job is suspicious when it does literally nothing.
        return true;
    }

    if (lhs->Phase != EJobPhase::Running) {
        return true;
    }

    bool wasActive = lhs->ProcessedInputRowCount < rhs->ProcessedInputRowCount;
    wasActive |= lhs->ProcessedInputUncompressedDataSize < rhs->ProcessedInputUncompressedDataSize;
    wasActive |= lhs->ProcessedInputCompressedDataSize < rhs->ProcessedInputCompressedDataSize;
    wasActive |= lhs->ProcessedOutputRowCount < rhs->ProcessedOutputRowCount;
    wasActive |= lhs->ProcessedOutputUncompressedDataSize < rhs->ProcessedOutputUncompressedDataSize;

    //! NB(psushin): output compressed data size is an estimate for a running job (due to async compression),
    //! so it can fluctuate (see #TEncodingWriter::GetCompressedSize).
    wasActive |= lhs->ProcessedOutputCompressedDataSize != rhs->ProcessedOutputCompressedDataSize;
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
    YT_VERIFY(jobSummary->Statistics);
    const auto& statistics = *jobSummary->Statistics;

    auto briefStatistics = New<TBriefJobStatistics>();
    briefStatistics->Phase = jobSummary->Phase;

    if (auto value = FindNumericValue(statistics, InputRowCountPath)) {
        briefStatistics->ProcessedInputRowCount = *value;
    }
    if (auto value = FindNumericValue(statistics, InputUncompressedDataSizePath)) {
        briefStatistics->ProcessedInputUncompressedDataSize = *value;
    }
    if (auto value = FindNumericValue(statistics, InputCompressedDataSizePath)) {
        briefStatistics->ProcessedInputCompressedDataSize = *value;
    }
    if (auto value = FindNumericValue(statistics, InputDataWeightPath)) {
        briefStatistics->ProcessedInputDataWeight = *value;
    }
    briefStatistics->InputPipeIdleTime = FindNumericValue(statistics, InputPipeIdleTimePath);
    briefStatistics->JobProxyCpuUsage = FindNumericValue(statistics, JobProxyCpuUsagePath);
    briefStatistics->Timestamp = statistics.GetTimestamp().value_or(TInstant::Now());

    auto outputPipeIdleTimes = GetOutputPipeIdleTimes(statistics);
    if (!outputPipeIdleTimes.empty()) {
        briefStatistics->OutputPipeIdleTime = 0;
        // This is a simplest way to achieve the desired result, although not the most fair one.
        for (const auto& [tableIndex, idleTime] : outputPipeIdleTimes) {
            briefStatistics->OutputPipeIdleTime = std::max<i64>(*briefStatistics->OutputPipeIdleTime, idleTime);
        }
    }

    // TODO(max42): GetTotalOutputDataStatistics is implemented very inefficiently (it creates THashMap containing
    // output data statistics per output table and then aggregates them). Rewrite it without any new allocations.
    auto outputDataStatistics = GetTotalOutputDataStatistics(statistics);
    briefStatistics->ProcessedOutputUncompressedDataSize = outputDataStatistics.uncompressed_data_size();
    briefStatistics->ProcessedOutputCompressedDataSize = outputDataStatistics.compressed_data_size();
    briefStatistics->ProcessedOutputRowCount = outputDataStatistics.row_count();

    return briefStatistics;
}

void ParseStatistics(TJobSummary* jobSummary, TInstant startTime, const TYsonString& lastObservedStatisticsYson)
{
    if (!jobSummary->StatisticsYson) {
        jobSummary->StatisticsYson = lastObservedStatisticsYson;
    }

    auto& statistics = jobSummary->Statistics;
    const auto& statisticsYson = jobSummary->StatisticsYson;
    if (statisticsYson) {
        statistics = ConvertTo<TStatistics>(statisticsYson);
        // NB: we should remove timestamp from the statistics as it becomes a YSON-attribute
        // when writing it to the event log, but top-level attributes are disallowed in table rows.
        statistics->SetTimestamp(std::nullopt);
    } else {
        statistics = TStatistics();
    }

    {
        auto endTime = jobSummary->FinishTime ? *jobSummary->FinishTime : TInstant::Now();
        auto duration = endTime - startTime;
        statistics->AddSample("/time/total", duration.MilliSeconds());
    }

    if (jobSummary->PrepareDuration) {
        statistics->AddSample("/time/prepare", jobSummary->PrepareDuration->MilliSeconds());
    }
    if (jobSummary->DownloadDuration) {
        statistics->AddSample("/time/artifacts_download", jobSummary->DownloadDuration->MilliSeconds());
    }
    if (jobSummary->PrepareRootFSDuration) {
        statistics->AddSample("/time/prepare_root_fs", jobSummary->PrepareRootFSDuration->MilliSeconds());
    }
    if (jobSummary->ExecDuration) {
        statistics->AddSample("/time/exec", jobSummary->ExecDuration->MilliSeconds());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TScheduleJobStatistics::RecordJobResult(const TControllerScheduleJobResult& scheduleJobResult)
{
    for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues()) {
        Failed[reason] += scheduleJobResult.Failed[reason];
    }
    Duration += scheduleJobResult.Duration;
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

} // namespace NYT::NControllerAgent
