#include "statistics.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/proto/session_service.pb.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/string/string_builder.h>

#include <ostream>

namespace NYT::NDistributedChunkSessionClient {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

bool IsNonnegative(const TDistributedChunkSessionProgress& progress)
{
    return IsComponentwiseLessOrEqual(TDistributedChunkSessionProgress{}, progress);
}

bool IsNonnegative(const TSessionSealSummary& summary)
{
    return summary.RecordCount >= 0 && summary.PhysicalCompressedDataSize >= 0;
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
    builder->AppendFormat(
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
