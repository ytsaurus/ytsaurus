#pragma once

#include "public.h"

#include <ytlib/misc/cache.h>
#include <ytlib/misc/property.h>
#include <ytlib/actions/action_queue.h>
#include <ytlib/actions/signal.h>
#include <ytlib/chunk_client/file_reader.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

//! Manages stored chunks.
class TChunkStore
    : public TRefCounted
{
public:
    typedef std::vector<TStoredChunkPtr> TChunks;
    typedef std::vector<TLocationPtr> TLocations;

    //! Constructs a new instance.
    TChunkStore(TDataNodeConfigPtr config, TBootstrap* bootstrap);

    //! Initializes the store.
    void Start();

    //! Registers a chunk.
    void RegisterChunk(TStoredChunkPtr chunk);
    
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

    void UpdateCellGuid(const TGuid& cellGuid);

    //! Storage locations.
    DEFINE_BYREF_RO_PROPERTY(TLocations, Locations);

    //! Raised when a chunk is added to the store.
    DEFINE_SIGNAL(void(TChunkPtr), ChunkAdded);

    //! Raised when a chunk is removed from the store.
    DEFINE_SIGNAL(void(TChunkPtr), ChunkRemoved);

private:
    void DoUpdateCellGuid();

    TDataNodeConfigPtr Config;
    TBootstrap* Bootstrap;

    typedef yhash_map<TChunkId, TStoredChunkPtr> TChunkMap;
    TChunkMap ChunkMap;

    TGuid CellGuid;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

