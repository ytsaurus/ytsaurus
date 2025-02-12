#pragma once

#include "public.h"
#include "chunk_owner_data_statistics.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkTreeStatistics
{
    //! Total number of rows in the tree.
    i64 RowCount = 0;

    //! Number of addressable rows in the tree. Typically equals to #RowCount but can be
    //! larger if some initial prefix of the rowset was trimmed.
    i64 LogicalRowCount = 0;

    //! Sum of uncompressed data sizes of chunks in the tree.
    i64 UncompressedDataSize = 0;

    //! Sum of compressed data sizes of chunks in the tree.
    i64 CompressedDataSize = 0;

    //! Sum of data weights of chunks in the tree. Equals -1 if chunk tree contains old chunks.
    i64 DataWeight = 0;

    //! Sum of data weights of all historical chunks in the tree.
    //! Can be larger than #DataWeight if some initial prefix of the rowset was trimmed.
    i64 LogicalDataWeight = 0;

    //! Disk space occupied on data nodes by regular chunks (without replication).
    i64 RegularDiskSpace = 0;

    //! Disk space occupied on data nodes by erasure chunks (including parity parts).
    i64 ErasureDiskSpace = 0;

    //! Total number of chunks in the tree.
    int ChunkCount = 0;

    //! Total number of chunk lists in the tree.
    int ChunkListCount = 0;

    //! Chunks and chunk views have zero ranks.
    //! Chunk lists have rank |1 + maxChildRank|, where |maxChildRank = 0| if there are no children.
    int Rank = 0;

    void Accumulate(const TChunkTreeStatistics& other);
    void Deaccumulate(const TChunkTreeStatistics& other);

    TChunkOwnerDataStatistics ToDataStatistics() const;

    void Persist(const NCellMaster::TPersistenceContext& context);

    bool operator == (const TChunkTreeStatistics& other) const;
};

void FormatValue(TStringBuilderBase* builder, const TChunkTreeStatistics& statistics, TStringBuf spec);

void Serialize(const TChunkTreeStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
