#pragma once

#include "common.h"

#include <ytlib/object_server/id.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

typedef i32 THolderId;
const i32 InvalidHolderId = -1;

typedef NObjectServer::TObjectId TChunkId;
extern TChunkId NullChunkId;

typedef NObjectServer::TObjectId TChunkListId;
extern TChunkListId NullChunkListId;

typedef NObjectServer::TObjectId TChunkTreeId;
extern TChunkTreeId NullChunkTreeId;

using NObjectServer::TTransactionId;
using NObjectServer::NullTransactionId;

typedef TGuid TJobId;

DECLARE_ENUM(EJobState,
    (Running)
    (Completed)
    (Failed)
);

DECLARE_ENUM(EJobType,
    (Replicate)
    (Remove)
);

////////////////////////////////////////////////////////////////////////////////

//! Represents an offset inside a chunk.
typedef i64 TBlockOffset;

DECLARE_ENUM(EChunkType,
    ((Unknown)(0))
    ((File)(1))
    ((Table)(2))
);

////////////////////////////////////////////////////////////////////////////////

//! Identifies a block.
/*!
 *  Each block is identified by its chunk id and block index (0-based).
 */
struct TBlockId
{
    //! TChunkId of the chunk where the block belongs.
    TChunkId ChunkId;

    //! An offset where the block starts.
    i32 BlockIndex;

    TBlockId(const TChunkId& chunkId, i32 blockIndex)
        : ChunkId(chunkId)
        , BlockIndex(blockIndex)
    { }

    //! Formats the id into the string (for debugging and logging purposes mainly).
    Stroka ToString() const
    {
        return Sprintf("%s:%d",
            ~ChunkId.ToString(),
            BlockIndex);
    }
};

//! Compares TBlockId s for equality.
inline bool operator==(const TBlockId& blockId1, const TBlockId& blockId2)
{
    return blockId1.ChunkId == blockId2.ChunkId &&
           blockId1.BlockIndex == blockId2.BlockIndex;
}

//! Compares TBlockId s for inequality.
inline bool operator!=(const TBlockId& blockId1, const TBlockId& blockId2)
{
    return !(blockId1 == blockId2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

//! A hasher for TBlockId.
template<>
struct hash<NYT::NChunkServer::TBlockId>
{
    i32 operator()(const NYT::NChunkServer::TBlockId& blockId) const
    {
        return (i32) THash<NYT::TGuid>()(blockId.ChunkId) * 497 + (i32) blockId.BlockIndex;
    }
};

////////////////////////////////////////////////////////////////////////////////

