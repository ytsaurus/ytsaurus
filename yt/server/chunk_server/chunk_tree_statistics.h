#pragma once

#include "public.h"

#include <core/yson/public.h>

#include <server/cell_master/public.h>

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

    //! Disk space occupied on data nodes by regular chunks (without replication).
    i64 RegularDiskSpace;

    //! Disk space occupied on data nodes by erasure chunks (including parity parts).
    i64 ErasureDiskSpace;

    //! Total number of chunks in the tree.
    i32 ChunkCount;

    //! Total number of chunk lists in the tree.
    i32 ChunkListCount;

    //! Distance to leaves (chunks) in edges. Leaves have rank zero.
    i32 Rank;

    TChunkTreeStatistics();

    void Accumulate(const TChunkTreeStatistics& other);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

};

void Serialize(const TChunkTreeStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
