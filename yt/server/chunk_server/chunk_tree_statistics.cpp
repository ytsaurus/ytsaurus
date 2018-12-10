#include "chunk_tree_statistics.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/serialize.h>

#include <yt/server/chunk_server/chunk_manager.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/convert.h>

namespace NYT::NChunkServer {

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
    Sealed = other.Sealed;

    if (DataWeight == -1 || other.DataWeight == -1) {
        DataWeight = -1;
    } else {
        DataWeight += other.DataWeight;
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
    // NB: Rank and Sealed are ignored intentionally since there's no way to
    // deaccumulate these values.

    if (DataWeight == -1 || other.DataWeight == -1) {
        DataWeight = -1;
    } else {
        DataWeight -= other.DataWeight;
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

void TChunkTreeStatistics::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, RowCount);
    Save(context, LogicalRowCount);
    Save(context, UncompressedDataSize);
    Save(context, CompressedDataSize);
    Save(context, DataWeight);
    Save(context, RegularDiskSpace);
    Save(context, ErasureDiskSpace);
    Save(context, ChunkCount);
    Save(context, LogicalChunkCount);
    Save(context, ChunkListCount);
    Save(context, Rank);
    Save(context, Sealed);
}

void TChunkTreeStatistics::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, RowCount);
    // COMPAT(babenko)
    if (context.GetVersion() >= 400) {
        Load(context, LogicalRowCount);
    } else {
        LogicalRowCount = RowCount;
    }
    Load(context, UncompressedDataSize);
    Load(context, CompressedDataSize);
    Load(context, DataWeight);
    Load(context, RegularDiskSpace);
    Load(context, ErasureDiskSpace);
    Load(context, ChunkCount);
    // COMPAT(babenko)
    if (context.GetVersion() >= 400) {
        Load(context, LogicalChunkCount);
    } else {
        LogicalChunkCount = ChunkCount;
    }
    Load(context, ChunkListCount);
    Load(context, Rank);
    Load(context, Sealed);
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
        Sealed == other.Sealed &&
        (DataWeight == -1 || other.DataWeight == -1 || DataWeight == other.DataWeight);
}

bool TChunkTreeStatistics::operator != (const TChunkTreeStatistics& other) const
{
    return !(*this == other);
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TChunkTreeStatistics& statistics)
{
    return ConvertToYsonString(statistics, EYsonFormat::Text).GetData();
}

void Serialize(const TChunkTreeStatistics& statistics, NYson::IYsonConsumer* consumer, const TChunkManagerPtr& chunkManager)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("row_count").Value(statistics.RowCount)
            .Item("logical_row_count").Value(statistics.LogicalRowCount)
            .Item("uncompressed_data_size").Value(statistics.UncompressedDataSize)
            .Item("compressed_data_size").Value(statistics.CompressedDataSize)
            .Item("data_weight").Value(statistics.DataWeight)
            .Item("regular_disk_space").Value(statistics.RegularDiskSpace)
            .Item("erasure_disk_space").Value(statistics.ErasureDiskSpace)
            .Item("chunk_count").Value(statistics.ChunkCount)
            .Item("logical_chunk_count").Value(statistics.LogicalChunkCount)
            .Item("chunk_list_count").Value(statistics.ChunkListCount)
            .Item("rank").Value(statistics.Rank)
            .Item("sealed").Value(statistics.Sealed)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
