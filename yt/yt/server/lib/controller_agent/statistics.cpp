#include "statistics.h"

#include <yt/yt/ytlib/chunk_client/traffic_meter.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/core/misc/statistics.h>
#include <yt/yt/core/misc/statistic_path.h>
#include <yt/yt/core/misc/collection_helpers.h>

#include <util/string/util.h>

namespace NYT::NControllerAgent {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NStatisticPath;

////////////////////////////////////////////////////////////////////////////////

const TStatisticPath InputPrefix = "/data/input"_SP;
const TStatisticPath OutputPrefix = "/data/output"_SP;
const TStatisticPath OutputPipePrefix = "/user_job/pipes/output"_SP;
const TStatisticPath IdleTimeSuffix = "/idle_time"_SP;

TDataStatistics GetTotalInputDataStatistics(const TStatistics& jobStatistics)
{
    TDataStatistics result;
    for (const auto& [path, summary] : jobStatistics.GetRangeByPrefix(InputPrefix)) {
        // TODO(pavook) check that we can get .Back() instead of dropping prefix.
        SetDataStatisticsField(result, path.Back(), summary.GetSum());
    }

    return result;
}

std::vector<TDataStatistics> GetOutputDataStatistics(const TStatistics& jobStatistics)
{
    std::vector<TDataStatistics> result;
    for (const auto& [path, summary] : jobStatistics.GetRangeByPrefix(OutputPrefix)) {
        auto [pathIt, outputPrefixIt] = std::ranges::mismatch(path, OutputPrefix);
        YT_VERIFY(outputPrefixIt == OutputPrefix.end());
        if (pathIt == path.end() || std::next(pathIt) == path.end()) {
            // Looks like a malformed path in /data/output, let's skip it.
            continue;
        }
        int tableIndex = a2i(TString(*pathIt));
        EnsureVectorIndex(result, tableIndex);
        SetDataStatisticsField(result[tableIndex], *(++pathIt), summary.GetSum());
    }

    return result;
}

THashMap<int, i64> GetOutputPipeIdleTimes(const TStatistics& jobStatistics)
{
    THashMap<int, i64> result;
    for (const auto& [path, summary] : jobStatistics.GetRangeByPrefix(OutputPipePrefix)) {
        auto [pathIt, prefixIt] = std::ranges::mismatch(path, OutputPipePrefix);
        // Note that path should start with OutputPipePrefix.
        YT_VERIFY(prefixIt == OutputPipePrefix.end());

        // TODO(pavook) change to `| std::views::reverse` when libc++ updates.
        auto [reversePathIt, reverseSuffixIt] = std::mismatch(path.rbegin(), path.rend(), IdleTimeSuffix.rbegin(), IdleTimeSuffix.rend());
        if (reverseSuffixIt != IdleTimeSuffix.rend()) {
            continue;
        }

        int tableIndex = a2i(TString(*reversePathIt));
        result[tableIndex] = summary.GetSum();
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void FillTrafficStatistics(
    const TStatisticPath& namePrefix,
    TStatistics& statistics,
    const NChunkClient::TTrafficMeterPtr& trafficMeter)
{
    statistics.AddSample(
        namePrefix / "traffic"_L / "duration_ms"_L,
        trafficMeter->GetDuration().MilliSeconds());

    // Empty data center names aren't allowed, so reducing a null data
    // center to an empty string is safe. And convenient :-)

    for (const auto& [optionalDataCenter, byteCount] : trafficMeter->GetInboundByteCountBySource()) {
        auto dataCenter = optionalDataCenter.value_or(TString());
        TStatisticPathLiteral fromSuffix("from_" + dataCenter);
        statistics.AddSample(namePrefix / "traffic"_L / "inbound"_L / fromSuffix, byteCount);
    }
    for (const auto& [optionalDataCenter, byteCount] : trafficMeter->GetOutboundByteCountByDestination()) {
        auto dataCenter = optionalDataCenter.value_or(TString());
        TStatisticPathLiteral toSuffix("to_" + dataCenter);
        statistics.AddSample(namePrefix / "traffic"_L / "outbound"_L / toSuffix, byteCount);
    }
    for (const auto& [direction, byteCount] : trafficMeter->GetByteCountByDirection()) {
        auto srcDataCenter = direction.first.value_or(TString());
        auto dstDataCenter = direction.second.value_or(TString());

        TStatisticPathLiteral routeSuffix(srcDataCenter + "_to_" + dstDataCenter);
        statistics.AddSample(namePrefix / "traffic"_L / routeSuffix, byteCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
