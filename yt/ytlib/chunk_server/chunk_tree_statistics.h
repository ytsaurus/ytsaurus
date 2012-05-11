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

    TChunkTreeStatistics()
        : RowCount(0)
        , UncompressedSize(0)
        , CompressedSize(0)
        , ChunkCount(0)
    { }

    void Accumulate(const TChunkTreeStatistics& other);

    void Save(TOutputStream* output) const;
    void Load(TInputStream* input);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
