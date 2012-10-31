#include "stdafx.h"
#include "chunk_tree_statistics.h"

#include <server/cell_master/bootstrap.h>

#include <server/chunk_server/chunk_manager.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

void TChunkTreeStatistics::Accumulate(const TChunkTreeStatistics& other)
{
    RowCount += other.RowCount;
    UncompressedSize += other.UncompressedSize;
    CompressedSize += other.CompressedSize;
    DiskSpace += other.DiskSpace;
    ChunkCount += other.ChunkCount;
    Rank = Max(Rank, other.Rank);
}

////////////////////////////////////////////////////////////////////////////////

void Save(const TChunkTreeStatistics& statistics, const NCellMaster::TSaveContext& context)
{
    auto* output = context.GetOutput();
    ::Save(output, statistics.RowCount);
    ::Save(output, statistics.UncompressedSize);
    ::Save(output, statistics.CompressedSize);
    ::Save(output, statistics.ChunkCount);
    ::Save(output, statistics.Rank);
    ::Save(output, statistics.DiskSpace);
}

void Load(TChunkTreeStatistics& statistics, const NCellMaster::TLoadContext& context)
{
    auto* input = context.GetInput();
    ::Load(input, statistics.RowCount);
    ::Load(input, statistics.UncompressedSize);
    ::Load(input, statistics.CompressedSize);
    ::Load(input, statistics.ChunkCount);
    ::Load(input, statistics.Rank);
    // COMPAT(babenko)
    if (context.GetVersion() >= 2) {
        ::Load(input, statistics.DiskSpace);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
