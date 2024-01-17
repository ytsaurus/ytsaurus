#include "job_helpers.h"
#include "job_info.h"

#include <yt/yt/server/controller_agent/controller_agent.h>
#include <yt/yt/server/controller_agent/config.h>

#include <yt/yt/server/lib/controller_agent/public.h>
#include <yt/yt/server/lib/controller_agent/statistics.h>

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkPools;
using namespace NYson;
using namespace NYTree;
using namespace NYPath;
using namespace NChunkClient;
using namespace NPhoenix;
using namespace NScheduler;
using namespace NJobTrackerClient;

////////////////////////////////////////////////////////////////////////////////

void TBriefJobStatistics::Persist(const TPersistenceContext& context)
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

    // TODO(max42): this maximization logic is not correct if statistics are truncated, but let
    // us ignore this for now.
    auto outputPipeIdleTimes = GetOutputPipeIdleTimes(statistics);
    if (!outputPipeIdleTimes.empty()) {
        briefStatistics->OutputPipeIdleTime = 0;
        // This is a simplest way to achieve the desired result, although not the most fair one.
        for (const auto& [tableIndex, idleTime] : outputPipeIdleTimes) {
            briefStatistics->OutputPipeIdleTime = std::max<i64>(*briefStatistics->OutputPipeIdleTime, idleTime);
        }
    }

    YT_VERIFY(jobSummary->TotalOutputDataStatistics);
    const auto& totalOutputDataStatistics = *jobSummary->TotalOutputDataStatistics;
    briefStatistics->ProcessedOutputUncompressedDataSize = totalOutputDataStatistics.uncompressed_data_size();
    briefStatistics->ProcessedOutputCompressedDataSize = totalOutputDataStatistics.compressed_data_size();
    briefStatistics->ProcessedOutputRowCount = totalOutputDataStatistics.row_count();

    return briefStatistics;
}

void UpdateJobletFromSummary(
    const TJobSummary& jobSummary,
    const TJobletPtr& joblet)
{
    // Note that TJoblet::{Controller,Job}Statistics are intentionally made shared const-pointers.
    // This allows thread-safe access to statistics from joblet with snapshot semantics, which
    // is useful for regular running job statistics updating from separate thread-pool.

    // Update job statistics.

    if (jobSummary.Statistics) {
        // We got fresh statistics, take them as job statistics.
        joblet->JobStatistics = jobSummary.Statistics;
        joblet->LastStatisticsUpdateTime = jobSummary.StatusTimestamp;
    }
    const auto& jobStatistics = joblet->JobStatistics;

    // Update controller statistics.

    auto controllerStatistics = std::make_shared<TStatistics>();

    auto endTime = std::max(
        jobSummary.FinishTime.value_or(TInstant::Now()),
        joblet->LastUpdateTime);
    auto duration = endTime - joblet->StartTime;

    controllerStatistics->ReplacePathWithSample("/time/total", duration.MilliSeconds());

    auto getCumulativeMemory = [] (i64 memory, TDuration period) {
        double cumulativeMemory = static_cast<double>(memory) * period.MilliSeconds();
        // Due to possible i64 overflow we consider cumulativeMemory in bytes * seconds.
        return static_cast<i64>(cumulativeMemory / 1000.0);
    };

    i64 jobProxyMemoryReserve = joblet->EstimatedResourceUsage.GetJobProxyMemory() * *joblet->JobProxyMemoryReserveFactor;

    controllerStatistics->ReplacePathWithSample(
        "/job_proxy/estimated_memory",
        joblet->EstimatedResourceUsage.GetJobProxyMemory());
    controllerStatistics->ReplacePathWithSample(
        "/job_proxy/memory_reserve",
        jobProxyMemoryReserve);
    controllerStatistics->ReplacePathWithSample(
        "/job_proxy/cumulative_estimated_memory",
        getCumulativeMemory(joblet->EstimatedResourceUsage.GetJobProxyMemory(), duration));
    controllerStatistics->ReplacePathWithSample(
        "/job_proxy/cumulative_memory_reserve",
        getCumulativeMemory(jobProxyMemoryReserve, duration));
    controllerStatistics->AddSample(
        "/job_proxy/memory_reserve_factor_x10000",
        static_cast<int>(1e4 * *joblet->JobProxyMemoryReserveFactor));

    auto addCumulativeMemoryStatistics = [&] (const TString& originalPath, const TString& cumulativePath) {
        if (auto usage = FindNumericValue(*jobStatistics, originalPath)) {
            controllerStatistics->ReplacePathWithSample(
                cumulativePath,
                getCumulativeMemory(*usage, duration));
        }
    };
    addCumulativeMemoryStatistics("/user_job/max_memory", "/user_job/cumulative_max_memory");
    addCumulativeMemoryStatistics("/user_job/memory_reserve", "/user_job/cumulative_memory_reserve");
    addCumulativeMemoryStatistics("/job_proxy/max_memory", "/job_proxy/cumulative_max_memory");

    joblet->ControllerStatistics = std::move(controllerStatistics);

    // Update other joblet fields.

    joblet->LastUpdateTime = jobSummary.StatusTimestamp;
    if (jobSummary.FinishTime) {
        joblet->FinishTime = *jobSummary.FinishTime;
    }
    if (const auto* runningJobSummary = dynamic_cast<const TRunningJobSummary*>(&jobSummary)) {
        joblet->Progress = runningJobSummary->Progress;
        joblet->StderrSize = runningJobSummary->StderrSize;
    } else if (const auto* abortedJobSummary = dynamic_cast<const TAbortedJobSummary*>(&jobSummary)) {
        // TODO(pogorelov): introduce a test for this logic.
        if (!abortedJobSummary->Scheduled) {
            joblet->FinishTime = joblet->StartTime;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TScheduleAllocationStatistics::TScheduleAllocationStatistics(int movingAverageWindowSize)
    : SuccessfulDurationMovingAverage_(movingAverageWindowSize)
{ }

void TScheduleAllocationStatistics::RecordJobResult(const TControllerScheduleAllocationResult& scheduleAllocationResult)
{
    for (auto reason : TEnumTraits<EScheduleAllocationFailReason>::GetDomainValues()) {
        Failed_[reason] += scheduleAllocationResult.Failed[reason];
    }
    TotalDuration_ += scheduleAllocationResult.Duration;
    ++Count_;

    if (scheduleAllocationResult.StartDescriptor) {
        SuccessfulDurationMovingAverage_.AddValue(scheduleAllocationResult.Duration);
    }
}

void TScheduleAllocationStatistics::SetMovingAverageWindowSize(int movingAverageWindowSize)
{
    SuccessfulDurationMovingAverage_.SetWindowSize(movingAverageWindowSize);
}

void TScheduleAllocationStatistics::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Failed_);
    Persist(context, TotalDuration_);
    Persist(context, Count_);
}

DECLARE_DYNAMIC_PHOENIX_TYPE(TScheduleAllocationStatistics, 0x1ba9c7e0);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
