#include "chunk_tree_statistics.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NChunkClient::NProto;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void TChunkTreeStatistics::Accumulate(const TChunkTreeStatistics& other)
{
    RowCount += other.RowCount;
    LogicalRowCount += other.LogicalRowCount;
    UncompressedDataSize += other.UncompressedDataSize;
    CompressedDataSize += other.CompressedDataSize;
    RegularDiskSpace += other.RegularDiskSpace;
    ErasureDiskSpace += other.ErasureDiskSpace;
    ChunkCount += other.ChunkCount;
    LogicalChunkCount += other.LogicalChunkCount;
    ChunkListCount += other.ChunkListCount;
    Rank = std::max(Rank, other.Rank);

    if (DataWeight == -1 || other.DataWeight == -1) {
        DataWeight = -1;
    } else {
        DataWeight += other.DataWeight;
    }
    if (LogicalDataWeight == -1 || other.LogicalDataWeight == -1) {
        LogicalDataWeight = -1;
    } else {
        LogicalDataWeight += other.LogicalDataWeight;
    }
}

void TChunkTreeStatistics::Deaccumulate(const TChunkTreeStatistics& other)
{
    RowCount -= other.RowCount;
    LogicalRowCount -= other.LogicalRowCount;
    UncompressedDataSize -= other.UncompressedDataSize;
    CompressedDataSize -= other.CompressedDataSize;
    RegularDiskSpace -= other.RegularDiskSpace;
    ErasureDiskSpace -= other.ErasureDiskSpace;
    ChunkCount -= other.ChunkCount;
    LogicalChunkCount -= other.LogicalChunkCount;
    ChunkListCount -= other.ChunkListCount;
    // NB: Rank is ignored intentionally since there's no way to deaccumulate it.

    if (DataWeight == -1 || other.DataWeight == -1) {
        DataWeight = -1;
    } else {
        DataWeight -= other.DataWeight;
    }
    if (LogicalDataWeight == -1 || other.LogicalDataWeight == -1) {
        LogicalDataWeight = -1;
    } else {
        LogicalDataWeight -= other.LogicalDataWeight;
    }
}

TDataStatistics TChunkTreeStatistics::ToDataStatistics() const
{
    TDataStatistics result;
    result.set_uncompressed_data_size(UncompressedDataSize);
    result.set_compressed_data_size(CompressedDataSize);
    result.set_data_weight(DataWeight);
    result.set_row_count(RowCount);
    result.set_chunk_count(ChunkCount);
    result.set_regular_disk_space(RegularDiskSpace);
    result.set_erasure_disk_space(ErasureDiskSpace);
    return result;
}

void TChunkTreeStatistics::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, RowCount);
    Persist(context, LogicalRowCount);
    Persist(context, UncompressedDataSize);
    Persist(context, CompressedDataSize);
    Persist(context, DataWeight);
    Persist(context, LogicalDataWeight);
    Persist(context, RegularDiskSpace);
    Persist(context, ErasureDiskSpace);
    Persist(context, ChunkCount);
    Persist(context, LogicalChunkCount);
    Persist(context, ChunkListCount);
    Persist(context, Rank);
}

bool TChunkTreeStatistics::operator == (const TChunkTreeStatistics& other) const
{
    return
        RowCount == other.RowCount &&
        LogicalRowCount == other.LogicalRowCount &&
        UncompressedDataSize == other.UncompressedDataSize &&
        CompressedDataSize == other.CompressedDataSize &&
        RegularDiskSpace == other.RegularDiskSpace &&
        ErasureDiskSpace == other.ErasureDiskSpace &&
        ChunkCount == other.ChunkCount &&
        LogicalChunkCount == other.LogicalChunkCount &&
        ChunkListCount == other.ChunkListCount &&
        Rank == other.Rank &&
        (DataWeight == -1 || other.DataWeight == -1 || DataWeight == other.DataWeight) &&
        (LogicalDataWeight == -1 || other.LogicalDataWeight == -1 || LogicalDataWeight == other.LogicalDataWeight);
}

bool TChunkTreeStatistics::operator != (const TChunkTreeStatistics& other) const
{
    return !(*this == other);
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TChunkTreeStatistics& statistics)
{
    return ConvertToYsonString(statistics, EYsonFormat::Text).ToString();
}

void Serialize(const TChunkTreeStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("row_count").Value(statistics.RowCount)
            .Item("logical_row_count").Value(statistics.LogicalRowCount)
            .Item("uncompressed_data_size").Value(statistics.UncompressedDataSize)
            .Item("compressed_data_size").Value(statistics.CompressedDataSize)
            .Item("data_weight").Value(statistics.DataWeight)
            .Item("logical_data_weight").Value(statistics.LogicalDataWeight)
            .Item("trimmed_data_weight").Value(statistics.LogicalDataWeight - statistics.DataWeight)
            .Item("regular_disk_space").Value(statistics.RegularDiskSpace)
            .Item("erasure_disk_space").Value(statistics.ErasureDiskSpace)
            .Item("chunk_count").Value(statistics.ChunkCount)
            .Item("logical_chunk_count").Value(statistics.LogicalChunkCount)
            .Item("chunk_list_count").Value(statistics.ChunkListCount)
            .Item("rank").Value(statistics.Rank)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
