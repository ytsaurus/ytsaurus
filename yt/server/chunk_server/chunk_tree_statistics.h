#pragma once

#include "public.h"

#include <server/cell_master/serialization_context.h>

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
    
    //! Disk space occupied on data nodes (with replication).
    i64 DiskSpace;

    //! Total number of chunks in the tree.
    i32 ChunkCount;

    //! Distance to leaves (chunks) in edges. Leaves have rank zero.
    i32 Rank;

    TChunkTreeStatistics()
        : RowCount(0)
        , UncompressedSize(0)
        , CompressedSize(0)
        , DiskSpace(0)
        , ChunkCount(0)
        , Rank(0)
    { }

    void Accumulate(const TChunkTreeStatistics& other);

};

void Save(const TChunkTreeStatistics& statistics, const NCellMaster::TSaveContext& context);
void Load(TChunkTreeStatistics& statistics, const NCellMaster::TLoadContext& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
