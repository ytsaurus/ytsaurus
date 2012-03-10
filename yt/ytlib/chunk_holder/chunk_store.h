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
    typedef yvector<TStoredChunkPtr> TChunks;
    typedef yvector<TLocationPtr> TLocations;

    //! Constructs a new instance.
    TChunkStore(TChunkHolderConfig* config, TBootstrap* bootstrap);

    //! Initializes the store.
    void Start();

    //! Registers a chunk.
    void RegisterChunk(TStoredChunk* chunk);
    
    //! Finds chunk by id. Returns NULL if no chunk exists.
    TStoredChunkPtr FindChunk(const TChunkId& chunkId) const;

    //! Physically removes the chunk.
    /*!
     *  This call also evicts the reader from the cache thus hopefully closing the file.
     */
    void RemoveChunk(TStoredChunk* chunk);

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

    //! Storage locations.
    DEFINE_BYREF_RO_PROPERTY(TLocations, Locations);

    //! Raised when a chunk is added to the store.
    DEFINE_SIGNAL(void(TChunk*), ChunkAdded);

    //! Raised when a chunk is removed from the store.
    DEFINE_SIGNAL(void(TChunk*), ChunkRemoved);

private:
    TChunkHolderConfigPtr Config;
    TBootstrap* Bootstrap;

    typedef yhash_map<TChunkId, TStoredChunkPtr> TChunkMap;
    TChunkMap ChunkMap;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

