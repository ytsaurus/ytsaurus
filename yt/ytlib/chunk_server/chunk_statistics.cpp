#include "stdafx.h"
#include "chunk_statistics.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

void TChunkStatistics::Accumulate(const TChunkStatistics& other)
{
    RowCount += other.RowCount;
    UncompressedSize += other.UncompressedSize;
}

void TChunkStatistics::Negate()
{
    RowCount = -RowCount;
    UncompressedSize = -UncompressedSize;
}

void TChunkStatistics::Save(TOutputStream* output) const
{
    ::Save(output, RowCount);
    ::Save(output, UncompressedSize);
}

void TChunkStatistics::Load(TInputStream* input)
{
    ::Load(input, RowCount);
    ::Load(input, UncompressedSize);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
