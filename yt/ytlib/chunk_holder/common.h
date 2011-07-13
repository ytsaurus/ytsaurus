#pragma once

#include "../misc/common.h"
#include "../logging/log.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TChunkHolderConfig
{
    int WindowSize;
    int CacheCapacity;
    TDuration LeaseTimeout;
    yvector<Stroka> Locations;

    TChunkHolderConfig()
        : WindowSize(256)
        , LeaseTimeout(TDuration::Seconds(10))
    {
        Locations.push_back(".");
    }

    void Read(const TJsonObject* jsonConfig);
};

////////////////////////////////////////////////////////////////////////////////

typedef TGUID TChunkId;
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
        static TGUIDHash hash;
        return hash(blockId.ChunkId) * 497 + (i32) blockId.Offset;
    }
};

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
