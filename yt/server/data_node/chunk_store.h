#pragma once

#include "public.h"

#include <core/misc/cache.h>
#include <core/misc/property.h>

#include <core/concurrency/action_queue.h>
#include <core/actions/signal.h>

#include <ytlib/chunk_client/file_reader.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages stored chunks.
class TChunkStore
    : public TRefCounted
{
public:
    typedef std::vector<TStoredChunkPtr> TChunks;
    typedef std::vector<TLocationPtr> TLocations;

    TChunkStore(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    void Initialize();

    //! Registers a just-written chunk.
    void RegisterNewChunk(TStoredChunkPtr chunk);

    //! Registers a chunk at startup.
    void RegisterExistingChunk(TStoredChunkPtr chunk);

    //! Finds chunk by id. Returns NULL if no chunk exists.
    TStoredChunkPtr FindChunk(const TChunkId& chunkId) const;

    //! Physically removes the chunk.
    /*!
     *  This call also evicts the reader from the cache thus hopefully closing the file.
     */
    TFuture<void> RemoveChunk(TStoredChunkPtr chunk);

    //! Calculates a storage location for a new chunk.
    /*!
     *  Among not full locations returns a random location having the minimum number
     *  of active sessions. Throws exception of all locations are full
     */
    TLocationPtr GetNewChunkLocation();

    //! Returns the list of all registered chunks.
    TChunks GetChunks() const;

    //! Returns the number of registered chunks.
    int GetChunkCount() const;

    const TGuid& GetCellGuid() const;

    void SetCellGuid(const TGuid& cellGuid);

    //! Storage locations.
    DEFINE_BYREF_RO_PROPERTY(TLocations, Locations);

    //! Raised when a chunk is added to the store.
    DEFINE_SIGNAL(void(TChunkPtr), ChunkAdded);

    //! Raised when a chunk is removed from the store.
    DEFINE_SIGNAL(void(TChunkPtr), ChunkRemoved);

private:
    TDataNodeConfigPtr Config;
    NCellNode::TBootstrap* Bootstrap;

    typedef yhash_map<TChunkId, TStoredChunkPtr> TChunkMap;
    TChunkMap ChunkMap;

    TGuid CellGuid;

    void DoSetCellGuid();
    void DoRegisterChunk(TStoredChunkPtr chunk);
    void OnLocationDisabled(TLocationPtr location);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

