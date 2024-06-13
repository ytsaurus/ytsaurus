#include "chunk_owner_data_statistics.h"

#include <yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

namespace NYT::NChunkServer {

using namespace NCypressServer;
using namespace NCellMaster;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

bool TChunkOwnerDataStatistics::IsDataWeightValid() const
{
    return DataWeight != -1;
}

TChunkOwnerDataStatistics& TChunkOwnerDataStatistics::operator+= (const TChunkOwnerDataStatistics& other)
{
    UncompressedDataSize += other.UncompressedDataSize;
    CompressedDataSize += other.CompressedDataSize;
    ChunkCount += other.ChunkCount;
    RowCount += other.RowCount;
    RegularDiskSpace += other.RegularDiskSpace;
    ErasureDiskSpace += other.ErasureDiskSpace;

    if (!IsDataWeightValid() || !other.IsDataWeightValid()) {
        DataWeight = -1;
    } else {
        DataWeight += other.DataWeight;
    }

    return *this;
}

TChunkOwnerDataStatistics TChunkOwnerDataStatistics::operator+ (const TChunkOwnerDataStatistics& other) const
{
    auto result = *this;
    result += other;
    return result;
}

bool TChunkOwnerDataStatistics::operator== (const TChunkOwnerDataStatistics& other) const
{
    return
        UncompressedDataSize == other.UncompressedDataSize &&
        CompressedDataSize == other.CompressedDataSize &&
        RowCount == other.RowCount &&
        ChunkCount == other.ChunkCount &&
        RegularDiskSpace == other.RegularDiskSpace &&
        ErasureDiskSpace == other.ErasureDiskSpace &&
        (
            !IsDataWeightValid() ||
            !other.IsDataWeightValid() ||
            DataWeight == other.DataWeight);
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TChunkOwnerDataStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .Item("chunk_count").Value(statistics.ChunkCount)
        .Item("row_count").Value(statistics.RowCount)
        .Item("uncompressed_data_size").Value(statistics.UncompressedDataSize)
        .Item("compressed_data_size").Value(statistics.CompressedDataSize)
        .Item("data_weight").Value(statistics.DataWeight)
        .Item("regular_disk_space").Value(statistics.RegularDiskSpace)
        .Item("erasure_disk_space").Value(statistics.ErasureDiskSpace)
    .EndMap();
}

void FormatValue(TStringBuilderBase* builder, const TChunkOwnerDataStatistics& statistics, TStringBuf /*spec*/)
{
    builder->AppendFormat(
        "{UncompressedDataSize: %v, CompressedDataSize: %v, DataWeight: %v, RowCount: %v, "
        "ChunkCount: %v, RegularDiskSpace: %v, ErasureDiskSpace: %v}",
        statistics.UncompressedDataSize,
        statistics.CompressedDataSize,
        statistics.DataWeight,
        statistics.RowCount,
        statistics.ChunkCount,
        statistics.RegularDiskSpace,
        statistics.ErasureDiskSpace);
}

TString ToString(const TChunkOwnerDataStatistics& statistics)
{
    return ToStringViaBuilder(statistics);
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TChunkOwnerDataStatistics* dataStatistics,
    const NChunkClient::NProto::TDataStatistics& protoDataStatistics)
{
    dataStatistics->UncompressedDataSize = FromProto<i64>(protoDataStatistics.uncompressed_data_size());
    dataStatistics->CompressedDataSize = FromProto<i64>(protoDataStatistics.compressed_data_size());
    dataStatistics->RowCount = FromProto<i64>(protoDataStatistics.row_count());
    dataStatistics->ChunkCount = FromProto<i64>(protoDataStatistics.chunk_count());
    dataStatistics->RegularDiskSpace = FromProto<i64>(protoDataStatistics.regular_disk_space());
    dataStatistics->ErasureDiskSpace = FromProto<i64>(protoDataStatistics.erasure_disk_space());
    dataStatistics->DataWeight = FromProto<i64>(protoDataStatistics.data_weight());
}

void ToProto(
    NChunkClient::NProto::TDataStatistics* protoDataStatistics,
    const TChunkOwnerDataStatistics& dataStatistics)
{
    protoDataStatistics->Clear();
    protoDataStatistics->set_uncompressed_data_size(dataStatistics.UncompressedDataSize);
    protoDataStatistics->set_compressed_data_size(dataStatistics.CompressedDataSize);
    protoDataStatistics->set_row_count(dataStatistics.RowCount);
    protoDataStatistics->set_chunk_count(dataStatistics.ChunkCount);
    protoDataStatistics->set_regular_disk_space(dataStatistics.RegularDiskSpace);
    protoDataStatistics->set_erasure_disk_space(dataStatistics.ErasureDiskSpace);
    protoDataStatistics->set_data_weight(dataStatistics.DataWeight);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
