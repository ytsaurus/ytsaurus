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
    typedef std::vector<IChunkPtr> TChunks;
    typedef std::vector<TLocationPtr> TLocations;

    TChunkStore(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    void Initialize();

    //! Registers a just-written chunk.
    void RegisterNewChunk(IChunkPtr chunk);

    //! Registers a chunk at startup.
    void RegisterExistingChunk(IChunkPtr chunk);

    //! Finds chunk by id. Returns NULL if no chunk exists.
    IChunkPtr FindChunk(const TChunkId& chunkId) const;

    //! Physically removes the chunk.
    /*!
     *  This call also evicts the reader from the cache thus hopefully closing the file.
     */
    TFuture<void> RemoveChunk(IChunkPtr chunk);

    //! Calculates a storage location for a new chunk.
    /*!
     *  Among locations that are not full returns a random one having the minimum number
     *  of active sessions. Throws exception if all locations are full.
     */
    TLocationPtr GetNewChunkLocation();

    //! Returns the list of all registered chunks.
    TChunks GetChunks() const;

    //! Returns the number of registered chunks.
    int GetChunkCount() const;

    //! Storage locations.
    DEFINE_BYREF_RO_PROPERTY(TLocations, Locations);

    //! Raised when a chunk is added to the store.
    DEFINE_SIGNAL(void(IChunkPtr), ChunkAdded);

    //! Raised when a chunk is removed from the store.
    DEFINE_SIGNAL(void(IChunkPtr), ChunkRemoved);

private:
    TDataNodeConfigPtr Config_;
    NCellNode::TBootstrap* Bootstrap_;

    yhash_map<TChunkId, IChunkPtr> ChunkMap_;

    void DoRegisterChunk(IChunkPtr chunk);
    void OnLocationDisabled(TLocationPtr location);

};

DEFINE_REFCOUNTED_TYPE(TChunkStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

