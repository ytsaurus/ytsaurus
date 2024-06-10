#include "chunk_tree_statistics.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
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

TChunkOwnerDataStatistics TChunkTreeStatistics::ToDataStatistics() const
{
    TChunkOwnerDataStatistics result;
    result.UncompressedDataSize = UncompressedDataSize;
    result.CompressedDataSize = CompressedDataSize;
    result.DataWeight = DataWeight;
    result.RowCount = RowCount;
    result.ChunkCount = ChunkCount;
    result.RegularDiskSpace = RegularDiskSpace;
    result.ErasureDiskSpace = ErasureDiskSpace;
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

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TChunkTreeStatistics& statistics, TStringBuf spec)
{
    FormatValue(builder, ConvertToYsonString(statistics, EYsonFormat::Text), spec);
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
