#include "statistics.h"

#include <yt/yt/ytlib/chunk_client/traffic_meter.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/core/misc/statistics.h>
#include <yt/yt/core/misc/collection_helpers.h>

#include <util/string/util.h>

namespace NYT::NJobTrackerClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

const TString InputPrefix = "/data/input";
const TString OutputPrefix = "/data/output";
const TString OutputPipePrefix = "/user_job/pipes/output";
const TString IdleTimeSuffix = "/idle_time";

TDataStatistics GetTotalInputDataStatistics(const TStatistics& jobStatistics)
{
    TDataStatistics result;
    for (const auto& [path, summary] : jobStatistics.GetRangeByPrefix(InputPrefix)) {
        SetDataStatisticsField(result, TStringBuf(path.begin() + 1 + InputPrefix.size(), path.end()), summary.GetSum());
    }

    return result;
}

std::vector<TDataStatistics> GetOutputDataStatistics(const TStatistics& jobStatistics)
{
    std::vector<TDataStatistics> result;
    for (const auto& [path, summary] : jobStatistics.GetRangeByPrefix(OutputPrefix)) {
        TStringBuf currentPath(path.begin() + OutputPrefix.size() + 1, path.end());
        size_t slashPos = currentPath.find("/");
        if (slashPos == TStringBuf::npos) {
            // Looks like a malformed path in /data/output, let's skip it.
            continue;
        }
        int tableIndex = a2i(TString(currentPath.substr(0, slashPos)));
        EnsureVectorIndex(result, tableIndex);
        SetDataStatisticsField(result[tableIndex], currentPath.substr(slashPos + 1), summary.GetSum());
    }

    return result;
}

THashMap<int, i64> GetOutputPipeIdleTimes(const TStatistics& jobStatistics)
{
    THashMap<int, i64> result;
    for (const auto& [path, summary] : jobStatistics.GetRangeByPrefix(OutputPipePrefix)) {
        // Note that path should contain at least OutputPipePrefix + '/'.
        YT_VERIFY(path.size() >= OutputPipePrefix.size() + 1);
        if (path.substr(path.size() - IdleTimeSuffix.size()) != IdleTimeSuffix) {
            continue;
        }
        int tableIndex = a2i(TString(path.begin() + OutputPipePrefix.size() + 1, path.end() - IdleTimeSuffix.size()));
        result[tableIndex] = summary.GetSum();
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

const TString ExecAgentTrafficStatisticsPrefix = "exec_agent";
const TString JobProxyTrafficStatisticsPrefix = "job_proxy";

void FillTrafficStatistics(
    const TString& namePrefix,
    TStatistics& statistics,
    const NChunkClient::TTrafficMeterPtr& trafficMeter)
{
    statistics.AddSample(
        Format("/%v/traffic/duration_ms", namePrefix),
        trafficMeter->GetDuration().MilliSeconds());

    // Empty data center names aren't allowed, so reducing a null data
    // center to an empty string is safe. And convenient :-)

    for (const auto& [optionalDataCenter, byteCount] : trafficMeter->GetInboundByteCountBySource()) {
        auto dataCenter = optionalDataCenter.value_or(TString());
        statistics.AddSample(
            Format("/%v/traffic/inbound/from_%v", namePrefix, dataCenter),
            byteCount);
    }
    for (const auto& [optionalDataCenter, byteCount] : trafficMeter->GetOutboundByteCountByDestination()) {
        auto dataCenter = optionalDataCenter.value_or(TString());
        statistics.AddSample(
            Format("/%v/traffic/outbound/to_%v", namePrefix, dataCenter),
            byteCount);
    }
    for (const auto& [direction, byteCount] : trafficMeter->GetByteCountByDirection()) {
        auto srcDataCenter = direction.first.value_or(TString());
        auto dstDataCenter = direction.second.value_or(TString());
        statistics.AddSample(
            Format("/%v/traffic/%v_to_%v", namePrefix, srcDataCenter, dstDataCenter),
            byteCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobTrackerClient
