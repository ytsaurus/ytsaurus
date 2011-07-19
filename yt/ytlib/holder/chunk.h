#pragma once

#include "../misc/common.h"
#include <util/system/file.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

typedef TGuid TChunkId;
typedef i64 TBlockOffset;

////////////////////////////////////////////////////////////////////////////////

struct TBlockId
{
    TChunkId ChunkId;
    TBlockOffset Offset;

    TBlockId(TChunkId chunkId, TBlockOffset offset)
        : ChunkId(chunkId)
        , Offset(offset)
    { }
};

inline bool operator==(const TBlockId& blockId1, const TBlockId& blockId2)
{
    return blockId1.ChunkId == blockId2.ChunkId &&
           blockId1.Offset == blockId2.Offset;
}

inline bool operator!=(const TBlockId& blockId1, const TBlockId& blockId2)
{
    return !(blockId1 == blockId2);
}

////////////////////////////////////////////////////////////////////////////////

struct TBlockIdHash
{
    i32 operator()(const TBlockId& blockId) const
    {
        static TGuidHash hash;
        return hash(blockId.ChunkId) * 497 + (i32) blockId.Offset;
    }
};

////////////////////////////////////////////////////////////////////////////////

}
