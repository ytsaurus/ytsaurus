#include "stdafx.h"
#include "chunk_tree_statistics.h"

#include <core/ytree/fluent.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/serialize.h>

#include <server/chunk_server/chunk_manager.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

void TChunkTreeStatistics::Accumulate(const TChunkTreeStatistics& other)
{
    RowCount += other.RowCount;
    UncompressedDataSize += other.UncompressedDataSize;
    CompressedDataSize += other.CompressedDataSize;
    DataWeight += other.DataWeight;
    RegularDiskSpace += other.RegularDiskSpace;
    ErasureDiskSpace += other.ErasureDiskSpace;
    ChunkCount += other.ChunkCount;
    ChunkListCount += other.ChunkListCount;
    Rank = std::max(Rank, other.Rank);
    Sealed = other.Sealed;
}

void TChunkTreeStatistics::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, RowCount);
    Save(context, UncompressedDataSize);
    Save(context, CompressedDataSize);
    Save(context, DataWeight);
    Save(context, RegularDiskSpace);
    Save(context, ErasureDiskSpace);
    Save(context, ChunkCount);
    Save(context, ChunkListCount);
    Save(context, Rank);
    Save(context, Sealed);
}

void TChunkTreeStatistics::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, RowCount);
    Load(context, UncompressedDataSize);
    Load(context, CompressedDataSize);
    Load(context, DataWeight);
    Load(context, RegularDiskSpace);
    Load(context, ErasureDiskSpace);
    Load(context, ChunkCount);
    Load(context, ChunkListCount);
    Load(context, Rank);
    // COMPAT(babenko)
    if (context.GetVersion() >= 100) {
        Load(context, Sealed);
    }
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TChunkTreeStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("row_count").Value(statistics.RowCount)
            .Item("uncompressed_data_size").Value(statistics.UncompressedDataSize)
            .Item("compressed_data_size").Value(statistics.CompressedDataSize)
            .Item("data_weight").Value(statistics.DataWeight)
            .Item("regular_disk_space").Value(statistics.RegularDiskSpace)
            .Item("erasure_disk_space").Value(statistics.ErasureDiskSpace)
            .Item("chunk_count").Value(statistics.ChunkCount)
            .Item("chunk_list_count").Value(statistics.ChunkListCount)
            .Item("rank").Value(statistics.Rank)
            .Item("sealed").Value(statistics.Sealed)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
