#pragma once

#include "chunk_holder.pb.h"
#include "chunk_manager_rpc.pb.h"

#include "../election/leader_lookup.h"
#include "../misc/guid.h"
#include "../misc/common.h"
#include "../logging/log.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

//! Describes a configuration of TChunkHolder.
struct TChunkHolderConfig
{
    //! Maximum number blocks in cache.
    int MaxCachedBlocks;

    //! Maximum number opened files in cache.
    int MaxCachedFiles;

    //! Upload session timeout.
    /*!
     * Some activity must be happening is a session on a regular basis (i.e. new
     * blocks uploaded or sent to other chunk holders). Otherwise
     * the session expires.
     */
    TDuration SessionTimeout;
    
    //! Paths to storage locations.
    yvector<Stroka> Locations;

    //! Masters configuration.
    /*!
     *  If no master addresses are given, the holder will operate in a standalone mode.
     */
    TLeaderLookup::TConfig Masters; 
    
    //! Period between consequent heartbeats.
    TDuration HeartbeatPeriod;

    //! Timeout for RPC requests.
    TDuration RpcTimeout;

    //! Port number to listen.
    int Port;

    //! Constructs a default instance.
    /*!
     *  By default, no master connection is configured. The holder will operate in
     *  a standalone mode, which only makes sense for testing purposes.
     */
    TChunkHolderConfig()
        : MaxCachedBlocks(1024)
        , MaxCachedFiles(256)
        , SessionTimeout(TDuration::Seconds(15))
        , HeartbeatPeriod(TDuration::Seconds(15))
        , RpcTimeout(TDuration::Seconds(5))
        , Port(9000)
    {
        Locations.push_back(".");
    }

    //! Reads configuration from JSON.
    void Read(TJsonObject* json);
};

////////////////////////////////////////////////////////////////////////////////

//! Identifies a chunk.
typedef TGuid TChunkId;
typedef TGuidHash TChunkIdHash;

//! Represents an offset inside a chunk.
typedef i64 TBlockOffset;

////////////////////////////////////////////////////////////////////////////////

//! Identifies a block.
/*!
 * Each block is identified by (chunkId, offset) pair.
 * Note that block's size is not part of its id.
 * Hence to fetch a block, the client must know its size.
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
            ~ChunkId.ToString(),
            Offset);
    }
};

//! Compares TBlockId s for equality.
inline bool operator==(const TBlockId& blockId1, const TBlockId& blockId2)
{
    return blockId1.ChunkId == blockId2.ChunkId &&
           blockId1.Offset == blockId2.Offset;
}

//! Compares TBlockId s for inequality.
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
        static TGuidHash hash;
        return hash(blockId.ChunkId) * 497 + (i32) blockId.Offset;
    }
};

////////////////////////////////////////////////////////////////////////////////

// TODO: to statistics.h/cpp
struct THolderStatistics
{
    THolderStatistics()
        : AvailableSpace(0)
        , UsedSpace(0)
        , ChunkCount(0)
    { }

    i64 AvailableSpace;
    i64 UsedSpace;
    i32 ChunkCount;

    static THolderStatistics FromProto(const NChunkManager::NProto::THolderStatistics& proto)
    {
        THolderStatistics result;
        result.AvailableSpace = proto.GetAvailableSpace();
        result.UsedSpace = proto.GetUsedSpace();
        result.ChunkCount = proto.GetChunkCount();
        return result;
    }

    NChunkManager::NProto::THolderStatistics ToProto() const
    {
        NChunkManager::NProto::THolderStatistics result;
        result.SetAvailableSpace(AvailableSpace);
        result.SetUsedSpace(UsedSpace);
        result.SetChunkCount(ChunkCount);
        return result;
    }

    Stroka ToString() const
    {
        return Sprintf("AvailableSpace: %" PRId64 ", UsedSpace: %" PRId64 ", ChunkCount: %d",
            AvailableSpace,
            UsedSpace,
            ChunkCount);
    }
};

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
