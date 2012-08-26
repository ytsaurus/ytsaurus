#include "stdafx.h"
#include "chunk_tree_statistics.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

void TChunkTreeStatistics::Accumulate(const TChunkTreeStatistics& other)
{
    RowCount += other.RowCount;
    UncompressedSize += other.UncompressedSize;
    CompressedSize += other.CompressedSize;
    ChunkCount += other.ChunkCount;
    Rank = Max(Rank, other.Rank);
}

void TChunkTreeStatistics::Save(TOutputStream* output) const
{
    ::Save(output, RowCount);
    ::Save(output, UncompressedSize);
    ::Save(output, CompressedSize);
    ::Save(output, ChunkCount);
    ::Save(output, Rank);
}

void TChunkTreeStatistics::Load(TInputStream* input)
{
    ::Load(input, RowCount);
    ::Load(input, UncompressedSize);
    ::Load(input, CompressedSize);
    ::Load(input, ChunkCount);
    ::Load(input, Rank);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
