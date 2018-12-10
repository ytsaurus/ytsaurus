#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/chunk_client/file_reader.h>
#include <yt/client/chunk_client/chunk_replica.h>
#include <yt/ytlib/chunk_client/session_id.h>

#include <yt/core/actions/signal.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/rw_spinlock.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/property.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages stored chunks.
/*!
 *  \note
 *  Thread affinity: ControlThread (unless indicated otherwise)
 */
class TChunkStore
    : public TRefCounted
{
public:
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

    //! Finds a chunk by id on the specified medium (or on the highest priority
    //! medium if #mediumIndex == AllMediaIndex).
    //! Returns |nullptr| if no chunk exists.
    //! NB: must not be called until the node is registered at master (because
    //! we lack medium name-to-index mapping until that).
    /*!
     *  \note
     *  Thread affinity: any
     */
    IChunkPtr FindChunk(const TChunkId& chunkId, int mediumIndex = NChunkClient::AllMediaIndex) const;

    //! Finds chunk by id on the specified medium (or on the highest priority
    //! medium if #mediumIndex == AllMediaIndex). Throws if no chunk exists.
    /*!
     *  \note
     *  Thread affinity: any
     */
    IChunkPtr GetChunkOrThrow(const TChunkId& chunkId, int mediumIndex = NChunkClient::AllMediaIndex) const;

    //! Returns the list of all registered chunks. These are not guaranteed to
    //! have unique IDs because a chunk may be stored on multiple media.
    /*!
     *  \note
     *  Thread affinity: any
     */
    std::vector<IChunkPtr> GetChunks() const;

    //! Returns the number of registered chunks. Chunks that are stored several
    //! times (on multiple media) counted several times.
    /*!
     *  \note
     *  Thread affinity: any
     */
    int GetChunkCount() const;

    //! Physically removes the chunk.
    /*!
     *  This call also evicts the reader from the cache thus hopefully closing the file.
     */
    TFuture<void> RemoveChunk(IChunkPtr chunk);

    //! Finds a suitable storage location for a new chunk.
    /*!
     *  The initial set of candidates consists of locations that are not full,
     *  support chunks of a given type, have requested medium type
     *  and don't currently throttle writes for a given workload.
     *
     *  If #TSessionOptions::PlacementId is null then
     *  a random candidate with the minimum number of active sessions is returned.
     *
     *  Otherwise the next (in round-robin order) candidate for this
     *  placement id is returned.
     *
     *  Throws exception if no suitable location could be found.
     */
    TStoreLocationPtr GetNewChunkLocation(
        const TSessionId& sessionId,
        const TSessionOptions& options);

    //! Storage locations.
    DEFINE_BYREF_RO_PROPERTY(std::vector<TStoreLocationPtr>, Locations);

    //! Raised when a chunk is added to the store.
    DEFINE_SIGNAL(void(IChunkPtr), ChunkAdded);

    //! Raised when a chunk is removed from the store.
    DEFINE_SIGNAL(void(IChunkPtr), ChunkRemoved);

private:
    const TDataNodeConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;
    const NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;

    struct TChunkEntry
    {
        IChunkPtr Chunk;
        i64 DiskSpace = 0;
    };

    NConcurrency::TReaderWriterSpinLock ChunkMapLock_;

    struct TPlacementInfo
    {
        int CurrentLocationIndex;
        std::multimap<TInstant, NChunkClient::TPlacementId>::iterator DeadlineIterator;
    };

    THashMap<NChunkClient::TPlacementId, TPlacementInfo> PlacementIdToInfo_;
    std::multimap<TInstant, NChunkClient::TPlacementId> DeadlineToPlacementId_;

    // A chunk may have multiple copies present on one node - as long as those
    // copies are placed on distinct media.
    // Such copies may have different sizes, too.
    THashMultiMap<TChunkId, TChunkEntry> ChunkMap_;

    using TChunkIdEntryPair = decltype(ChunkMap_)::value_type;

    bool CanStartNewSession(
        const TStoreLocationPtr& location,
        int mediumIndex,
        const TWorkloadDescriptor& workloadDescriptor);

    void DoRegisterChunk(const IChunkPtr& chunk);

    //! Returns an already stored chunk that has same ID and location medium
    //! name as #chunk. Returns |nullptr| if there's no such chunk.
    //! NB. Unlike #FindChunk(), this doesn't use medium name-to-index mapping.
    TChunkEntry FindExistingChunk(IChunkPtr chunk) const;

    //! Updates #oldChunk's entry with info about #newChunk and returns that info.
    TChunkEntry DoUpdateChunk(IChunkPtr oldChunk, IChunkPtr newChunk);

    TChunkEntry DoEraseChunk(IChunkPtr chunk);

    static TChunkEntry BuildEntry(IChunkPtr chunk);
    IChunkPtr CreateFromDescriptor(const TStoreLocationPtr& location, const TChunkDescriptor& descriptor);

    TPlacementInfo* GetOrCreatePlacementInfo(const NChunkClient::TPlacementId& placementId);
    void ExpirePlacementInfos();

    void OnProfiling();

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

};

DEFINE_REFCOUNTED_TYPE(TChunkStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

