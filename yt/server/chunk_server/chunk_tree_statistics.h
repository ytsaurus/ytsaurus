#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkTreeStatistics
{
    //! Total number of rows in the tree.
    i64 RowCount;
    
    //! Sum of uncompressed data sizes of chunks in the tree.
    i64 UncompressedDataSize;
    
    //! Sum of compressed data sizes of chunks in the tree.
    i64 CompressedDataSize;
    
    //! Sum of data weights of chunks in the tree.
    i64 DataWeight;

    //! Disk space occupied on data nodes (without replication).
    i64 DiskSpace;

    //! Total number of chunks in the tree.
    i32 ChunkCount;

    //! Distance to leaves (chunks) in edges. Leaves have rank zero.
    i32 Rank;

    TChunkTreeStatistics();

    void Accumulate(const TChunkTreeStatistics& other);

};

void Serialize(const TChunkTreeStatistics& statistics, NYTree::IYsonConsumer* consumer);

void Save(const TChunkTreeStatistics& statistics, const NCellMaster::TSaveContext& context);
void Load(TChunkTreeStatistics& statistics, const NCellMaster::TLoadContext& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
