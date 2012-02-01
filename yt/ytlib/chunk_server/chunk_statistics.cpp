#include "stdafx.h"
#include "chunk_statistics.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

void TChunkStatistics::Accumulate(const TChunkStatistics& other)
{
    RowCount += other.RowCount;
    UncompressedSize += other.UncompressedSize;
    CompressedSize += other.CompressedSize;
}

void TChunkStatistics::Negate()
{
    RowCount = -RowCount;
    UncompressedSize = -UncompressedSize;
    CompressedSize = -CompressedSize;
}

void TChunkStatistics::Save(TOutputStream* output) const
{
    ::Save(output, RowCount);
    ::Save(output, UncompressedSize);
    ::Save(output, CompressedSize);
}

void TChunkStatistics::Load(TInputStream* input)
{
    ::Load(input, RowCount);
    ::Load(input, UncompressedSize);
    ::Load(input, CompressedSize);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
