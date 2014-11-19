#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/action_queue.h>

#include <core/actions/signal.h>

#include <ytlib/chunk_client/file_reader.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages stored chunks.
/*!
 *  \note
 *  Thread affinity: ControlThread
 */
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

    //! Registers a chunk found during startup.
    void RegisterExistingChunk(IChunkPtr chunk);

    //! Triggers another round of master notification for a chunk that is already registered.
    /*!
     *  Used for journal chunks that initially get registered (with "active" replica type)
     *  when a session starts and subsequently get re-registered (with "unsealed" replica type)
     *  with the session finishes. Finally, when such a chunk is sealed it gets re-registered again
     *  (with "sealed" replica type).
     */
    void UpdateExistingChunk(IChunkPtr chunk);

    //! Unregisters the chunk but does not remove any of its files.
    void UnregisterChunk(IChunkPtr chunk);

    //! Finds chunk by id. Returns |nullptr| if no chunk exists.
    IChunkPtr FindChunk(const TChunkId& chunkId) const;

    //! Finds chunk by id. Throws if no chunk exists.
    IChunkPtr GetChunkOrThrow(const TChunkId& chunkId) const;

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

    struct TChunkEntry
    {
        IChunkPtr Chunk;
        i64 DiskSpace = 0;
    };

    yhash_map<TChunkId, TChunkEntry> ChunkMap_;


    void DoRegisterChunk(const TChunkEntry& entry);

    void OnLocationDisabled(TLocationPtr location, const TError& reason);

    static TChunkEntry BuildEntry(IChunkPtr chunk);
    IChunkPtr CreateFromDescriptor(TLocationPtr location, const TChunkDescriptor& descriptor);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

};

DEFINE_REFCOUNTED_TYPE(TChunkStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

