#pragma once

#include "../misc/common.h"
#include "../misc/string.h"
#include "../logging/log.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

//! Describes a configuration of TChunkHolder.
struct TChunkHolderConfig
{
    //! Maximum number blocks in cache (not counting those locked by smart pointers).
    int CacheCapacity;

    //! Upload session timeout.
    /*!
     * Some activity must be happening is a session on a regular basis (i.e. new
     * blocks uploaded or sent to other chunk holders). Otherwise
     * the session expires.
     */
    TDuration SessionTimeout;
    
    //! Paths to storage locations.
    yvector<Stroka> Locations;

    //! Constructs a default instance.
    TChunkHolderConfig()
        : SessionTimeout(TDuration::Seconds(10))
    {
        Locations.push_back(".");
    }

    void Read(const TJsonObject* jsonConfig);
};

////////////////////////////////////////////////////////////////////////////////

//! Identified a chunk.
typedef TGUID TChunkId;

//! Represents an offset inside a chunk.
typedef i64 TBlockOffset;

////////////////////////////////////////////////////////////////////////////////

//! Identifies a block.
/*!
 * Each block is identified by (chunkId, offset) pair.
 * Note that block's size is not part of its id.
 * Hence, in order to fetch a block, the client must know its size.
 */
struct TBlockId
{
    //! TChunkId of the chunk where the block belongs.
    TChunkId ChunkId;

    //! An offset where the block starts.
    TBlockOffset Offset;

    TBlockId(TChunkId chunkId, TBlockOffset offset)
        : ChunkId(chunkId)
        , Offset(offset)
    { }

    //! Formats the id into the string (for debugging and logging purposes mainly).
    Stroka ToString() const
    {
        return Sprintf("%s:%" PRId64,
            ~StringFromGuid(ChunkId),
            Offset);
    }
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

//! An hasher for TBlockId.
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

} // namespace NChunkHolder
} // namespace NYT
