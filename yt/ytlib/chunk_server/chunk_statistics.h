#pragma once

#include "common.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkStatistics
{
    i64 RowCount;
    i64 UncompressedSize;
    i64 CompressedSize;

    TChunkStatistics()
        : RowCount(0)
        , UncompressedSize(0)
        , CompressedSize(0)
    { }

    void Accumulate(const TChunkStatistics& other);
    void Negate();

    void Save(TOutputStream* output) const;
    void Load(TInputStream* input);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
