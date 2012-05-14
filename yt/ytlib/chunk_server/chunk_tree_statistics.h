#pragma once

#include "public.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkTreeStatistics
{
    i64 RowCount;
    i64 UncompressedSize;
    i64 CompressedSize;
    i32 ChunkCount;
    i32 Rank; // Rank is a distance to leaves (chunks) in edges. Chunks have rank = 0.

    TChunkTreeStatistics()
        : RowCount(0)
        , UncompressedSize(0)
        , CompressedSize(0)
        , ChunkCount(0)
        , Rank(0)
    { }

    void Accumulate(const TChunkTreeStatistics& other);

    void Save(TOutputStream* output) const;
    void Load(TInputStream* input);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
