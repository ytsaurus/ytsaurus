#include "input_statistics_collector.h"

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

namespace NYT::NControllerAgent {

using namespace NChunkClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TInputStatistics::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, ChunkCount);
    PHOENIX_REGISTER_FIELD(2, RowCount);
    PHOENIX_REGISTER_FIELD(3, ValueCount);
    PHOENIX_REGISTER_FIELD(4, DataWeight);
    PHOENIX_REGISTER_FIELD(5, CompressedDataSize);
    PHOENIX_REGISTER_FIELD(6, UncompressedDataSize);
    PHOENIX_REGISTER_FIELD(7, PrimaryDataWeight);
    PHOENIX_REGISTER_FIELD(8, ForeignDataWeight);
    PHOENIX_REGISTER_FIELD(9, PrimaryCompressedDataSize);
    PHOENIX_REGISTER_FIELD(10, ForeignCompressedDataSize);
    PHOENIX_REGISTER_FIELD(11, CompressionRatio);
    PHOENIX_REGISTER_FIELD(12, DataWeightRatio);
}

PHOENIX_DEFINE_TYPE(TInputStatistics);

////////////////////////////////////////////////////////////////////////////////

template <class TChunk>
void AddChunkImpl(TInputStatistics& statistics, const TChunk& chunk, bool isPrimary)
{
    if (isPrimary) {
        statistics.PrimaryDataWeight += chunk->GetDataWeight();
        statistics.PrimaryCompressedDataSize += chunk->GetCompressedDataSize();
    } else {
        statistics.ForeignDataWeight += chunk->GetDataWeight();
        statistics.ForeignCompressedDataSize += chunk->GetCompressedDataSize();
    }

    statistics.UncompressedDataSize += chunk->GetUncompressedDataSize();
    statistics.RowCount += chunk->GetRowCount();
    statistics.CompressedDataSize += chunk->GetCompressedDataSize();
    statistics.DataWeight += chunk->GetDataWeight();
    ++statistics.ChunkCount;
}

void TInputStatisticsCollector::AddChunk(const TInputChunkPtr& inputChunk, bool isPrimary) noexcept
{
    AddChunkImpl(Statistics_, inputChunk, isPrimary);
    Statistics_.ValueCount += inputChunk->GetValuesPerRow() * inputChunk->GetRowCount();
    TotalInputDataWeight_ += inputChunk->GetTotalDataWeight();
}

void TInputStatisticsCollector::AddChunk(const TLegacyDataSlicePtr& dataSlice, bool isPrimary) noexcept
{
    AddChunkImpl(Statistics_, dataSlice, isPrimary);
    Statistics_.ValueCount += dataSlice->GetValueCount();
    TotalInputDataWeight_ += dataSlice->GetDataWeight();
}

TInputStatistics TInputStatisticsCollector::Finish() && noexcept
{
    Statistics_.CompressionRatio = static_cast<double>(Statistics_.CompressedDataSize) / Statistics_.DataWeight;
    Statistics_.DataWeightRatio = static_cast<double>(TotalInputDataWeight_) / Statistics_.UncompressedDataSize;

    TotalInputDataWeight_ = 0;
    return std::exchange(Statistics_, TInputStatistics());
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TInputStatistics& statistics, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{ChunkCount: %v, RowCount: %v, ValueCount: %v, DataWeight: %v, CompressedDataSize: %v, UncompressedDataSize: %v,"
        " PrimaryDataWeight: %v, ForeignDataWeight: %v, PrimaryCompressedDataSize: %v, ForeignCompressedDataSize: %v,"
        " CompressionRatio: %v, DataWeightRatio: %v}",
        statistics.ChunkCount,
        statistics.RowCount,
        statistics.ValueCount,
        statistics.DataWeight,
        statistics.CompressedDataSize,
        statistics.UncompressedDataSize,
        statistics.PrimaryDataWeight,
        statistics.ForeignDataWeight,
        statistics.PrimaryCompressedDataSize,
        statistics.ForeignCompressedDataSize,
        statistics.CompressionRatio,
        statistics.DataWeightRatio);
}

void Serialize(const TInputStatistics& statistics, TFluentMap& fluent)
{
    fluent
        .Item("chunk_count").Value(statistics.ChunkCount)
        .Item("uncompressed_data_size").Value(statistics.UncompressedDataSize)
        .Item("compressed_data_size").Value(statistics.CompressedDataSize)
        .Item("data_weight").Value(statistics.DataWeight)
        .Item("row_count").Value(statistics.RowCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
