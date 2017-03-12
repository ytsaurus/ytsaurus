#include "job_helpers.h"

namespace NYT {
namespace NScheduler {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////

bool CheckJobActivity(
    const TBriefJobStatisticsPtr& lhs,
    const TBriefJobStatisticsPtr& rhs,
    i64 cpuUsageThreshold,
    double inputPipeIdleTimeFraction)
{
    bool wasActive = lhs->ProcessedInputRowCount < rhs->ProcessedInputRowCount;
    wasActive |= lhs->ProcessedInputUncompressedDataSize < rhs->ProcessedInputUncompressedDataSize;
    wasActive |= lhs->ProcessedInputCompressedDataSize < rhs->ProcessedInputCompressedDataSize;
    wasActive |= lhs->ProcessedOutputRowCount < rhs->ProcessedOutputRowCount;
    wasActive |= lhs->ProcessedOutputUncompressedDataSize < rhs->ProcessedOutputUncompressedDataSize;
    wasActive |= lhs->ProcessedOutputCompressedDataSize < rhs->ProcessedOutputCompressedDataSize;
    if (lhs->JobProxyCpuUsage && rhs->JobProxyCpuUsage) {
        wasActive |= *lhs->JobProxyCpuUsage + cpuUsageThreshold < *rhs->JobProxyCpuUsage;
    }
    if (lhs->InputPipeIdleTime && rhs->InputPipeIdleTime && lhs->Timestamp < rhs->Timestamp) {
        wasActive |= (*rhs->InputPipeIdleTime - *lhs->InputPipeIdleTime) < (rhs->Timestamp - lhs->Timestamp).MilliSeconds() * inputPipeIdleTimeFraction;
    }
    return wasActive;
}

TBriefJobStatisticsPtr BuildBriefStatistics(std::unique_ptr<TJobSummary> jobSummary)
{
    YCHECK(jobSummary->Statistics);
    const auto& statistics = *jobSummary->Statistics;

    auto briefStatistics = New<TBriefJobStatistics>();
    briefStatistics->ProcessedInputRowCount = GetNumericValue(statistics, "/data/input/row_count");
    briefStatistics->ProcessedInputUncompressedDataSize = GetNumericValue(statistics, "/data/input/uncompressed_data_size");
    briefStatistics->ProcessedInputCompressedDataSize = GetNumericValue(statistics, "/data/input/compressed_data_size");
    briefStatistics->InputPipeIdleTime = FindNumericValue(statistics, "/user_job/pipes/input/idle_time");
    briefStatistics->JobProxyCpuUsage = FindNumericValue(statistics, "/job_proxy/cpu/user");
    briefStatistics->Timestamp = statistics.GetTimestamp().Get(TInstant::Now());

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

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
