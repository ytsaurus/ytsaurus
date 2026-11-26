#include "job_statistics_wire.h"

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/statistic_path.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/misc/cast.h>

#include <algorithm>

namespace NYT::NControllerAgent {
namespace {

using namespace NStatisticPath;
using namespace NYTree;
using NChunkClient::NProto::TDataStatistics;

////////////////////////////////////////////////////////////////////////////////

template <class TCallback>
void VisitDataStatistics(const TDataStatistics& statistics, TCallback callback)
{
    callback("chunk_count"_L, statistics.chunk_count());
    callback("row_count"_L, statistics.row_count());
    callback("uncompressed_data_size"_L, statistics.uncompressed_data_size());
    callback("compressed_data_size"_L, statistics.compressed_data_size());
    callback("data_weight"_L, statistics.data_weight());
    callback("regular_disk_space"_L, statistics.regular_disk_space());
    callback("erasure_disk_space"_L, statistics.erasure_disk_space());
    callback("unmerged_row_count"_L, statistics.unmerged_row_count());
    callback("unmerged_data_weight"_L, statistics.unmerged_data_weight());
    callback("encoded_row_batch_count"_L, statistics.encoded_row_batch_count());
    callback("encoded_columnar_batch_count"_L, statistics.encoded_columnar_batch_count());
}

void RestoreDataStatistics(
    TNonNullPtr<TStatistics> statistics,
    const TStatisticPath& prefix,
    const TDataStatistics& dataStatistics)
{
    auto path = prefix;
    VisitDataStatistics(dataStatistics, [&] (const TStatisticPathLiteral& field, i64 value) {
        path.Append(field);
        if (statistics->Data().contains(path)) {
            auto printablePath = path.Path();
            std::ranges::replace(printablePath, Delimiter, '/');
            THROW_ERROR_EXCEPTION("Omitted data statistic %v is present in job statistics YSON", printablePath)
                .With("path", path);
        }
        statistics->AddSample(path, value);
        path.PopBack();
    });
}

TStatisticPath GetOutputPrefix(int index)
{
    return "/data/output"_SP / TStatisticPathLiteral(ToString(index));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

void AddJobDataStatistics(
    TNonNullPtr<TStatistics> statistics,
    const TDataStatistics& inputDataStatistics,
    const std::vector<TDataStatistics>& outputDataStatistics,
    bool hasInput,
    int outputCount)
{
    YT_VERIFY(outputCount >= 0 && outputCount <= std::ssize(outputDataStatistics));
    if (hasInput) {
        RestoreDataStatistics(statistics, "/data/input"_SP, inputDataStatistics);
    }
    for (int index = 0; index < outputCount; ++index) {
        RestoreDataStatistics(statistics, GetOutputPrefix(index), outputDataStatistics[index]);
    }
}

void EncodeJobStatisticsForHeartbeat(
    TNonNullPtr<NProto::TJobStatus> status,
    const NYson::TYsonString& statisticsYson,
    EJobStatisticsWireFlags wireFlags,
    bool inputDataStatisticsOmitted,
    int outputDataStatisticsOmittedCount)
{
    YT_VERIFY(TEnumTraits<EJobStatisticsWireFlags>::IsValidValue(wireFlags));

    status->set_statistics(statisticsYson.ToString());
    if (wireFlags == EJobStatisticsWireFlags::None) {
        return;
    }
    status->set_statistics_wire_flags(ToUnderlying(wireFlags));
    if (Any(wireFlags & EJobStatisticsWireFlags::OmitDataStatistics)) {
        status->set_input_data_statistics_omitted_from_yson(inputDataStatisticsOmitted);
        status->set_output_data_statistics_omitted_from_yson_count(outputDataStatisticsOmittedCount);
    }
}

TStatistics DecodeJobStatisticsFromHeartbeat(const NProto::TJobStatus& status)
{
    if (!status.has_statistics()) {
        THROW_ERROR_EXCEPTION("Job statistics YSON is absent");
    }
    auto wireFlags = TryCheckedEnumCast<EJobStatisticsWireFlags>(status.statistics_wire_flags());
    if (!wireFlags) {
        THROW_ERROR_EXCEPTION("Unsupported job statistics wire flags %v", status.statistics_wire_flags());
    }
    auto statisticsYson = NYson::TYsonStringBuf(status.statistics());
    auto statistics = Any(*wireFlags & EJobStatisticsWireFlags::Compact)
        ? ParseCompactStatistics(statisticsYson)
        : ConvertTo<TStatistics>(statisticsYson);
    if (None(*wireFlags & EJobStatisticsWireFlags::OmitDataStatistics)) {
        return statistics;
    }

    auto outputOmittedCount = status.output_data_statistics_omitted_from_yson_count();
    if (outputOmittedCount < 0 || outputOmittedCount > status.output_data_statistics_size()) {
        THROW_ERROR_EXCEPTION("Invalid omitted output data statistics count %v", outputOmittedCount);
    }
    if (status.input_data_statistics_omitted_from_yson()) {
        if (!status.has_total_input_data_statistics()) {
            THROW_ERROR_EXCEPTION("Omitted input data statistics protobuf is absent");
        }
        RestoreDataStatistics(GetPtr(statistics), "/data/input"_SP, status.total_input_data_statistics());
    }
    for (int index = 0; index < outputOmittedCount; ++index) {
        RestoreDataStatistics(GetPtr(statistics), GetOutputPrefix(index), status.output_data_statistics(index));
    }
    return statistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
