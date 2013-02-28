#include "stdafx.h"
#include "chunk_tree_statistics.h"

#include <ytlib/ytree/fluent.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/serialization_context.h>

#include <server/chunk_server/chunk_manager.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

TChunkTreeStatistics::TChunkTreeStatistics()
    : RowCount(0)
    , UncompressedDataSize(0)
    , CompressedDataSize(0)
    , DataWeight(0)
    , DiskSpace(0)
    , ChunkCount(0)
    , ChunkListCount(0)
    , Rank(0)
{ }

void TChunkTreeStatistics::Accumulate(const TChunkTreeStatistics& other)
{
    RowCount += other.RowCount;
    UncompressedDataSize += other.UncompressedDataSize;
    CompressedDataSize += other.CompressedDataSize;
    DataWeight += other.DataWeight;
    DiskSpace += other.DiskSpace;
    ChunkCount += other.ChunkCount;
    ChunkListCount += other.ChunkListCount;
    Rank = std::max(Rank, other.Rank);
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
            .Item("disk_space").Value(statistics.DiskSpace)
            .Item("chunk_count").Value(statistics.ChunkCount)
            .Item("chunk_list_count").Value(statistics.ChunkListCount)
            .Item("rank").Value(statistics.Rank)
        .EndMap();
}

void Save(const NCellMaster::TSaveContext& context, const TChunkTreeStatistics& statistics)
{
    auto* output = context.GetOutput();
    ::Save(output, statistics.RowCount);
    ::Save(output, statistics.UncompressedDataSize);
    ::Save(output, statistics.CompressedDataSize);
    ::Save(output, statistics.DataWeight);
    ::Save(output, statistics.DiskSpace);
    ::Save(output, statistics.ChunkCount);
    ::Save(output, statistics.ChunkListCount);
    ::Save(output, statistics.Rank);
}

void Load(const NCellMaster::TLoadContext& context, TChunkTreeStatistics& statistics)
{
    auto* input = context.GetInput();
    ::Load(input, statistics.RowCount);
    ::Load(input, statistics.UncompressedDataSize);
    ::Load(input, statistics.CompressedDataSize);
    ::Load(input, statistics.DataWeight);
    ::Load(input, statistics.DiskSpace);
    ::Load(input, statistics.ChunkCount);
    ::Load(input, statistics.ChunkListCount);
    ::Load(input, statistics.Rank);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
