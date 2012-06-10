#pragma once

#include "public.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkTreeStatistics
{
    //! Total number of rows in the tree.
    i64 RowCount;
    
    //! Sum of uncompressed sizes of chunks in the tree.
    i64 UncompressedSize;
    
    //! Sum of compressed sizes of chunks in the tree.
    i64 CompressedSize;
    
    //! Total number of chunks in the tree.
    int ChunkCount;

    //! Distance to leaves (chunks) in edges. Leaves have rank zero.
    int Rank;

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
