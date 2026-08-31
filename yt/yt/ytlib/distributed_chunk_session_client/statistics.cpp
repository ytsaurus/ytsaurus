#include "statistics.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/proto/session_service.pb.h>

#include <yt/yt/core/phoenix/type_def.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/library/numeric/util.h>

#include <library/cpp/yt/string/format.h>
#include <library/cpp/yt/string/string_builder.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <ostream>

namespace NYT::NDistributedChunkSessionClient {

using namespace NYson;
using namespace NYTree;

namespace {

////////////////////////////////////////////////////////////////////////////////

i64 MultiplyAndDivideApproximately(i64 lhs, i64 rhs, i64 divisor)
{
    YT_VERIFY(lhs >= 0);
    YT_VERIFY(rhs >= 0);
    YT_VERIFY(divisor > 0);

    double result = static_cast<double>(lhs) * rhs / divisor;
    YT_VERIFY(
        std::isfinite(result) &&
        result <= static_cast<double>(std::numeric_limits<i64>::max()));
    return SignedSaturationConversion(result);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool IsNonnegative(const TDistributedChunkSessionProgress& progress)
{
    return IsComponentwiseLessOrEqual(TDistributedChunkSessionProgress{}, progress);
}

bool IsNonnegative(const TSessionSealSummary& summary)
{
    return summary.RecordCount >= 0 && summary.PhysicalCompressedDataSize >= 0;
}

void VerifyNonnegative(const TDistributedChunkSessionProgress& progress)
{
    YT_VERIFY(IsNonnegative(progress));
}

void VerifyAtLeastOneUnitPerRecord(const TDistributedChunkSessionProgress& progress)
{
    YT_VERIFY(
        progress.DataWeight >= progress.RecordCount &&
        progress.CompressedDataSize >= progress.RecordCount &&
        progress.UncompressedDataSize >= progress.RecordCount &&
        progress.RowCount >= progress.RecordCount);
}

bool IsComponentwiseLessOrEqual(
    const TDistributedChunkSessionProgress& lhs,
    const TDistributedChunkSessionProgress& rhs)
{
    return lhs.DataWeight <= rhs.DataWeight &&
        lhs.CompressedDataSize <= rhs.CompressedDataSize &&
        lhs.UncompressedDataSize <= rhs.UncompressedDataSize &&
        lhs.RecordCount <= rhs.RecordCount &&
        lhs.RowCount <= rhs.RowCount;
}

TDistributedChunkSessionProgress operator+(
    const TDistributedChunkSessionProgress& lhs,
    const TDistributedChunkSessionProgress& rhs)
{
    auto checkedAdd = [] (i64 lhs, i64 rhs) {
        YT_VERIFY(!(
            (rhs > 0 && lhs > std::numeric_limits<i64>::max() - rhs) ||
            (rhs < 0 && lhs < std::numeric_limits<i64>::min() - rhs)));

        return lhs + rhs;
    };

    return {
        .DataWeight = checkedAdd(lhs.DataWeight, rhs.DataWeight),
        .CompressedDataSize = checkedAdd(lhs.CompressedDataSize, rhs.CompressedDataSize),
        .UncompressedDataSize = checkedAdd(lhs.UncompressedDataSize, rhs.UncompressedDataSize),
        .RecordCount = checkedAdd(lhs.RecordCount, rhs.RecordCount),
        .RowCount = checkedAdd(lhs.RowCount, rhs.RowCount),
    };
}

TDistributedChunkSessionProgress& operator+=(
    TDistributedChunkSessionProgress& lhs,
    const TDistributedChunkSessionProgress& rhs)
{
    lhs = lhs + rhs;
    return lhs;
}

TDistributedChunkSessionProgress operator-(
    const TDistributedChunkSessionProgress& lhs,
    const TDistributedChunkSessionProgress& rhs)
{
    auto checkedSubtract = [] (i64 lhs, i64 rhs) {
        YT_VERIFY(!(
            (rhs > 0 && lhs < std::numeric_limits<i64>::min() + rhs) ||
            (rhs < 0 && lhs > std::numeric_limits<i64>::max() + rhs)));

        return lhs - rhs;
    };

    return {
        .DataWeight = checkedSubtract(lhs.DataWeight, rhs.DataWeight),
        .CompressedDataSize = checkedSubtract(lhs.CompressedDataSize, rhs.CompressedDataSize),
        .UncompressedDataSize = checkedSubtract(lhs.UncompressedDataSize, rhs.UncompressedDataSize),
        .RecordCount = checkedSubtract(lhs.RecordCount, rhs.RecordCount),
        .RowCount = checkedSubtract(lhs.RowCount, rhs.RowCount),
    };
}

std::pair<TDistributedChunkSessionProgress, TDistributedChunkSessionProgress> Split(
    const TDistributedChunkSessionProgress& progress,
    i64 prefixRecordCount)
{
    YT_VERIFY(prefixRecordCount >= 0 && prefixRecordCount <= progress.RecordCount);
    YT_VERIFY(progress.RecordCount > 0);
    VerifyAtLeastOneUnitPerRecord(progress);

    auto getPrefixStatistic = [&] (i64 value) -> i64 {
        if (prefixRecordCount == 0) {
            return 0;
        }
        if (prefixRecordCount == progress.RecordCount) {
            return value;
        }

        // Clamping keeps every component at or above the record count on both halves: the
        // lower bound covers the prefix, the upper bound reserves the suffix its share.
        // The range is nonempty because the component is at least the record count.
        return std::clamp(
            std::min(
                value,
                MultiplyAndDivideApproximately(value, prefixRecordCount, progress.RecordCount)),
            prefixRecordCount,
            value - (progress.RecordCount - prefixRecordCount));
    };

    TDistributedChunkSessionProgress prefix{
        .DataWeight = getPrefixStatistic(progress.DataWeight),
        .CompressedDataSize = getPrefixStatistic(progress.CompressedDataSize),
        .UncompressedDataSize = getPrefixStatistic(progress.UncompressedDataSize),
        .RecordCount = prefixRecordCount,
        .RowCount = getPrefixStatistic(progress.RowCount),
    };
    return {prefix, progress - prefix};
}

TDistributedChunkSessionProgress Extrapolate(
    const TDistributedChunkSessionProgress& sample,
    i64 recordCount,
    i64 compressedDataSize)
{
    YT_VERIFY(compressedDataSize >= recordCount);
    YT_VERIFY(recordCount >= 0);
    YT_VERIFY(sample.RecordCount > 0);
    VerifyAtLeastOneUnitPerRecord(sample);

    // The exact ratio is at least the record count for every component of the sample;
    // the floor only covers rounding at the boundary.
    auto extrapolateStatistic = [&] (i64 value) {
        YT_VERIFY(value >= 0);

        return std::max(
            recordCount,
            MultiplyAndDivideApproximately(value, recordCount, sample.RecordCount));
    };

    return {
        .DataWeight = extrapolateStatistic(sample.DataWeight),
        .CompressedDataSize = compressedDataSize,
        .UncompressedDataSize = extrapolateStatistic(sample.UncompressedDataSize),
        .RecordCount = recordCount,
        .RowCount = extrapolateStatistic(sample.RowCount),
    };
}

void FormatValue(
    TStringBuilderBase* builder,
    const TSessionSealSummary& summary,
    TStringBuf /*spec*/)
{
    builder->AppendFormat(
        "{RecordCount: %v, PhysicalCompressedDataSize: %v}",
        summary.RecordCount,
        summary.PhysicalCompressedDataSize);
}

void FormatValue(
    TStringBuilderBase* builder,
    const TDistributedChunkSessionProgress& progress,
    TStringBuf /*spec*/)
{
    Format(
        builder,
        "{DataWeight: %v, CompressedDataSize: %v, UncompressedDataSize: %v, RecordCount: %v, RowCount: %v}",
        progress.DataWeight,
        progress.CompressedDataSize,
        progress.UncompressedDataSize,
        progress.RecordCount,
        progress.RowCount);
}

void PrintTo(
    const TDistributedChunkSessionProgress& progress,
    std::ostream* os)
{
    TStringBuilder builder;
    FormatValue(&builder, progress, /*spec*/ {});
    *os << builder.Flush();
}

void PrintTo(
    const TSessionSealSummary& summary,
    std::ostream* os)
{
    TStringBuilder builder;
    FormatValue(&builder, summary, /*spec*/ {});
    *os << builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TWriteRecordStatistics* protoStatistics,
    const TDistributedChunkSessionWriteStatistics& statistics)
{
    protoStatistics->set_data_weight(statistics.DataWeight);
    protoStatistics->set_uncompressed_data_size(statistics.UncompressedDataSize);
    protoStatistics->set_row_count(statistics.RowCount);
}

void FromProto(
    TDistributedChunkSessionWriteStatistics* statistics,
    const NProto::TWriteRecordStatistics& protoStatistics)
{
    statistics->DataWeight = protoStatistics.data_weight();
    statistics->UncompressedDataSize = protoStatistics.uncompressed_data_size();
    statistics->RowCount = protoStatistics.row_count();
}

void ToProto(
    NProto::TSessionProgress* protoProgress,
    const TDistributedChunkSessionProgress& progress)
{
    protoProgress->set_data_weight(progress.DataWeight);
    protoProgress->set_compressed_data_size(progress.CompressedDataSize);
    protoProgress->set_uncompressed_data_size(progress.UncompressedDataSize);
    protoProgress->set_record_count(progress.RecordCount);
    protoProgress->set_row_count(progress.RowCount);
}

void FromProto(
    TDistributedChunkSessionProgress* progress,
    const NProto::TSessionProgress& protoProgress)
{
    progress->DataWeight = protoProgress.data_weight();
    progress->CompressedDataSize = protoProgress.compressed_data_size();
    progress->UncompressedDataSize = protoProgress.uncompressed_data_size();
    progress->RecordCount = protoProgress.record_count();
    progress->RowCount = protoProgress.row_count();
}

void Serialize(
    const TDistributedChunkSessionProgress& progress,
    IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("data_weight").Value(progress.DataWeight)
            .Item("compressed_data_size").Value(progress.CompressedDataSize)
            .Item("uncompressed_data_size").Value(progress.UncompressedDataSize)
            .Item("record_count").Value(progress.RecordCount)
            .Item("row_count").Value(progress.RowCount)
        .EndMap();
}

void TDistributedChunkSessionProgress::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, DataWeight);
    PHOENIX_REGISTER_FIELD(2, CompressedDataSize);
    PHOENIX_REGISTER_FIELD(3, UncompressedDataSize);
    PHOENIX_REGISTER_FIELD(4, RecordCount);
    PHOENIX_REGISTER_FIELD(5, RowCount);
}

PHOENIX_DEFINE_TYPE(TDistributedChunkSessionProgress);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
