#include "statistics.h"

#include <yt/ytlib/chunk_client/traffic_meter.h>

#include <yt/client/chunk_client/data_statistics.h>

#include <yt/core/misc/statistics.h>

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
    for (const auto& pair : jobStatistics.GetRangeByPrefix(InputPrefix)) {
        SetDataStatisticsField(result, TStringBuf(pair.first.begin() + 1 + InputPrefix.size(), pair.first.end()), pair.second.GetSum());
    }

    return result;
}

THashMap<int, TDataStatistics> GetOutputDataStatistics(const TStatistics& jobStatistics)
{
    THashMap<int, TDataStatistics> result;
    for (const auto& pair : jobStatistics.GetRangeByPrefix(OutputPrefix)) {
        TStringBuf currentPath(pair.first.begin() + OutputPrefix.size() + 1, pair.first.end());
        size_t slashPos = currentPath.find("/");
        if (slashPos == TStringBuf::npos) {
            // Looks like a malformed path in /data/output, let's skip it.
            continue;
        }
        int tableIndex = a2i(TString(currentPath.substr(0, slashPos)));
        SetDataStatisticsField(result[tableIndex], currentPath.substr(slashPos + 1), pair.second.GetSum());
    }

    return result;
}

THashMap<int, i64> GetOutputPipeIdleTimes(const TStatistics& jobStatistics)
{
    THashMap<int, i64> result;
    for (const auto& pair : jobStatistics.GetRangeByPrefix(OutputPipePrefix)) {
        const auto& path = pair.first;
        // Note that path should contain at least OutputPipePrefix + '/'.
        YT_VERIFY(path.size() >= OutputPipePrefix.size() + 1);
        if (path.substr(path.size() - IdleTimeSuffix.size()) != IdleTimeSuffix) {
            continue;
        }
        int tableIndex = a2i(TString(path.begin() + OutputPipePrefix.size() + 1, path.end() - IdleTimeSuffix.size()));
        result[tableIndex] = pair.second.GetSum();
    }

    return result;
};

TDataStatistics GetTotalOutputDataStatistics(const TStatistics& jobStatistics)
{
    TDataStatistics result;
    for (const auto& pair : GetOutputDataStatistics(jobStatistics)) {
        result += pair.second;
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

    for (const auto& pair : trafficMeter->GetInboundByteCountBySource()) {
        auto dataCenter = pair.first ? pair.first : TString();
        auto byteCount = pair.second;
        statistics.AddSample(
            Format("/%v/traffic/inbound/from_%v", namePrefix, dataCenter),
            byteCount);
    }
    for (const auto& pair : trafficMeter->GetOutboundByteCountByDestination()) {
        auto dataCenter = pair.first ? pair.first : TString();
        auto byteCount = pair.second;
        statistics.AddSample(
            Format("/%v/traffic/outbound/to_%v", namePrefix, dataCenter),
            byteCount);
    }
    for (const auto& pair : trafficMeter->GetByteCountByDirection()) {
        auto srcDataCenter = pair.first.first ? pair.first.first : TString();
        auto dstDataCenter = pair.first.second ? pair.first.second : TString();
        auto byteCount = pair.second;
        statistics.AddSample(
            Format("/%v/traffic/%v_to_%v", namePrefix, srcDataCenter, dstDataCenter),
            byteCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobTrackerClient
