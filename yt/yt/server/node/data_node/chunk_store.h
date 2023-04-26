#pragma once

#include "location.h"

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/misc/public.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <util/generic/hash_multi_map.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct IChunkStoreHost
    : public TRefCounted
{
    virtual void ScheduleMasterHeartbeat() = 0;
    virtual NObjectClient::TCellId GetCellId() = 0;
    virtual void SubscribePopulateAlerts(TCallback<void(std::vector<TError>*)> alerts) = 0;
    virtual NClusterNode::TMasterEpoch GetMasterEpoch() = 0;
    virtual INodeMemoryTrackerPtr GetMemoryUsageTracker() = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkStoreHost)

IChunkStoreHostPtr CreateChunkStoreHost(NClusterNode::IBootstrapBase* bootstrap);

////////////////////////////////////////////////////////////////////////////////

//! Manages stored chunks.
/*!
 *  \note
 *  Thread affinity: any (unless indicated otherwise)
 */
class TChunkStore
    : public TRefCounted
{
public:
    TChunkStore(
        TDataNodeConfigPtr config,
        NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        IInvokerPtr controlInvoker,
        TChunkContextPtr chunkContext,
        IChunkStoreHostPtr chunkStoreHost);

    //! Scans locations for chunks and registers them.
    /*!
     *  \note
     *  Thread affinity: Control
     */
    void Initialize();

    TFuture<void> InitializeLocation(const TStoreLocationPtr& location);

    void Shutdown();

    void UpdateConfig(const TDataNodeDynamicConfigPtr& config);

    //! Registers a just-written chunk unless the chunk already exists or location is disabled.
    void RegisterNewChunk(
        const IChunkPtr& chunk,
        const ISessionPtr& session,
        TLockedChunkGuard lockedChunkGuard);

    //! Triggers another round of master notification for a chunk that is already registered.
    /*!
     *  Used for journal chunks that initially get registered (with "active" replica type)
     *  when a session starts and subsequently get re-registered (with "unsealed" replica type)
     *  with the session finishes. Finally, when such a chunk is sealed it gets re-registered again
     *  (with "sealed" replica type).
     */
    void UpdateExistingChunk(const IChunkPtr& chunk);

    //! Unregisters the chunk but does not remove any of its files.
    void UnregisterChunk(const IChunkPtr& chunk, bool force);

    //! Finds a chunk by id on the specified medium (or on the highest priority
    //! medium if #mediumIndex == AllMediaIndex).
    //! Returns |nullptr| if no chunk exists.
    //! NB: must not be called until the node is registered at master (because
    //! we lack medium name-to-index mapping until that).
    /*!
     *  \note
     *  Thread affinity: any
     */
    IChunkPtr FindChunk(TChunkId chunkId, int mediumIndex = NChunkClient::AllMediaIndex) const;

    //! Finds chunk by id on the specified medium (or on the highest priority
    //! medium if #mediumIndex == AllMediaIndex). Throws if no chunk exists.
    /*!
     *  \note
     *  Thread affinity: any
     */
    IChunkPtr GetChunkOrThrow(TChunkId chunkId, int mediumIndex = NChunkClient::AllMediaIndex) const;

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

    //! Get registered chunks by location.
    std::vector<IChunkPtr> GetLocationChunks(const TChunkLocationPtr& location);

    //! Physically removes or move to trash the location chunk. This method called with registering in location actions.
    /*!
     *  This call also evicts the reader from the cache thus hopefully closing the file.
     */
    TFuture<void> RemoveChunk(const IChunkPtr& chunk);

    //! Triggers medium change for all chunks in location.
    void ChangeLocationMedium(const TChunkLocationPtr& location, int oldMediumIndex);

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
     *
     *  \note
     *  Thread affinity: any
     */
    std::tuple<TStoreLocationPtr, TLockedChunkGuard> AcquireNewChunkLocation(
        TSessionId sessionId,
        const TSessionOptions& options);

    bool ShouldPublishDisabledLocations();

    //! Storage locations.
    DEFINE_BYREF_RO_PROPERTY(std::vector<TStoreLocationPtr>, Locations);

    //! Raised when a chunk is added to the store.
    DEFINE_SIGNAL(void(const IChunkPtr&), ChunkAdded);

    //! Raised when a chunk is removed from the store.
    DEFINE_SIGNAL(void(const IChunkPtr&), ChunkRemoved);

    //! Raised when a chunk is removal is scheduled.
    DEFINE_SIGNAL(void(const IChunkPtr&), ChunkRemovalScheduled);

    //! Raised when an underlying medium for a chunk changed.
    DEFINE_SIGNAL(void(const IChunkPtr& chunk, int oldMediumIndex), ChunkMediumChanged);

private:
    const TDataNodeConfigPtr Config_;
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    const IInvokerPtr ControlInvoker_;
    const TChunkContextPtr ChunkContext_;
    const IChunkStoreHostPtr ChunkStoreHost_;
    const NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;

    TDataNodeDynamicConfigPtr DynamicConfig_;

    struct TChunkEntry
    {
        IChunkPtr Chunk;
        i64 DiskSpace = 0;
    };

    struct TPlacementInfo
    {
        int CurrentLocationIndex;
        std::multimap<TInstant, NChunkClient::TPlacementId>::iterator DeadlineIterator;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, PlacementLock_);
    THashMap<NChunkClient::TPlacementId, TPlacementInfo> PlacementIdToInfo_;
    std::multimap<TInstant, NChunkClient::TPlacementId> DeadlineToPlacementId_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ChunkMapLock_);
    // A chunk may have multiple copies present on one node - as long as those
    // copies are placed on distinct media.
    // Such copies may have different sizes, too.
    THashMultiMap<TChunkId, TChunkEntry> ChunkMap_;

    bool CanStartNewSession(
        const TStoreLocationPtr& location,
        int mediumIndex);

    void DoRegisterExistingChunk(const IChunkPtr& chunk);

    void DoRegisterExistingJournalChunk(const IChunkPtr& chunk);

    void FinishChunkRegistration(const IChunkPtr& chunk);

    void OnChunkRegistered(const IChunkPtr& chunk);

    //! Returns an already stored chunk that has same ID and location medium
    //! name as #chunk. Returns |nullptr| if there's no such chunk.
    //! NB. Unlike #FindChunk(), this doesn't use medium name-to-index mapping.
    TChunkEntry DoFindExistingChunk(const IChunkPtr& chunk) const;

    //! Updates #oldChunk's entry with info about #newChunk and returns that info.
    TChunkEntry DoUpdateChunk(const IChunkPtr& oldChunk, const IChunkPtr& newChunk);

    TChunkEntry DoEraseChunk(const IChunkPtr& chunk);

    static TChunkEntry BuildChunkEntry(const IChunkPtr& chunk);
    IChunkPtr CreateFromDescriptor(const TStoreLocationPtr& location, const TChunkDescriptor& descriptor);

    TPlacementInfo* GetOrCreatePlacementInfo(NChunkClient::TPlacementId placementId);
    void ExpirePlacementInfos();

    void OnProfiling();

    void ReconfigureLocation(const TChunkLocationPtr& location);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

DEFINE_REFCOUNTED_TYPE(TChunkStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
