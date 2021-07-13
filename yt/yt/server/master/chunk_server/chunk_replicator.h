#pragma once

#include "private.h"
#include "chunk.h"
#include "job_controller.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/lib/misc/max_min_balancer.h>

#include <yt/yt/server/master/node_tracker_server/data_center.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/library/erasure/impl/public.h>

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/optional.h>
#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/small_set.h>

#include <yt/yt/core/profiling/timing.h>

#include <functional>
#include <deque>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicator
    : public IJobController
{
public:
    TChunkReplicator(
        TChunkManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap,
        TChunkPlacementPtr chunkPlacement,
        TJobRegistryPtr jobRegistry);

    ~TChunkReplicator();

    void Start(
        TChunk* blobFrontChunk,
        int blobChunkCount,
        TChunk* journalFrontChunk,
        int journalChunkCount);
    void Stop();

    void OnNodeDisposed(TNode* node);

    // 'On all of the media' chunk states. E.g. LostChunks contain chunks that
    // have been lost on all of the media.
    DEFINE_BYREF_RO_PROPERTY(THashSet<TChunk*>, LostChunks);
    DEFINE_BYREF_RO_PROPERTY(THashSet<TChunk*>, LostVitalChunks);
    DEFINE_BYREF_RO_PROPERTY(THashSet<TChunk*>, DataMissingChunks);
    DEFINE_BYREF_RO_PROPERTY(THashSet<TChunk*>, ParityMissingChunks);
    DEFINE_BYREF_RO_PROPERTY(TOldestPartMissingChunkSet, OldestPartMissingChunks, TChunkPartLossTimeComparer(0));
    // Medium-wise unsafely placed chunks: all replicas are on transient media
    // (and requisitions of these chunks demand otherwise).
    DEFINE_BYREF_RO_PROPERTY(THashSet<TChunk*>, PrecariousChunks);
    DEFINE_BYREF_RO_PROPERTY(THashSet<TChunk*>, PrecariousVitalChunks);

    // 'On any medium'. E.g. UnderreplicatedChunks contain chunks that are
    // underreplicated on at least one medium.
    DEFINE_BYREF_RO_PROPERTY(THashSet<TChunk*>, UnderreplicatedChunks);
    DEFINE_BYREF_RO_PROPERTY(THashSet<TChunk*>, OverreplicatedChunks);
    DEFINE_BYREF_RO_PROPERTY(THashSet<TChunk*>, QuorumMissingChunks);
    // Rack-wise unsafely placed chunks.
    DEFINE_BYREF_RO_PROPERTY(THashSet<TChunk*>, UnsafelyPlacedChunks);

    void OnChunkDestroyed(TChunk* chunk);
    void OnReplicaRemoved(
        TNode* node,
        TChunkPtrWithIndexes chunkWithIndexes,
        ERemoveReplicaReason reason);

    void ScheduleChunkRefresh(TChunk* chunk);
    void ScheduleNodeRefresh(TNode* node);
    void ScheduleGlobalChunkRefresh(
        TChunk* blobFrontChunk,
        int blobChunkCount,
        TChunk* journalFrontChunk,
        int journalChunkCount);

    void ScheduleUnknownReplicaRemoval(
        TNode* node,
        const NChunkClient::TChunkIdWithIndexes& chunkdIdWithIndexes);
    void ScheduleReplicaRemoval(
        TNode* node,
        TChunkPtrWithIndexes chunkWithIndexes);

    void ScheduleRequisitionUpdate(TChunk* chunk);
    void ScheduleRequisitionUpdate(TChunkList* chunkList);

    void TouchChunk(TChunk* chunk);

    TMediumMap<EChunkStatus> ComputeChunkStatuses(TChunk* chunk);

    void ScheduleJobs(
        TNode* node,
        NNodeTrackerClient::NProto::TNodeResources* resourceUsage,
        const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
        std::vector<TJobPtr>* jobsToStart);

    bool IsReplicatorEnabled();
    bool IsSealerEnabled();
    bool IsRefreshEnabled();
    bool IsRequisitionUpdateEnabled();

    bool IsEnabled();

    void OnProfiling(NProfiling::TSensorBuffer* buffer) const;

    // IJobController implementation.
    virtual void ScheduleJobs(IJobSchedulingContext* context) override;

    virtual void OnJobWaiting(const TJobPtr& job, IJobControllerCallbacks* callbacks) override;
    virtual void OnJobRunning(const TJobPtr& job, IJobControllerCallbacks* callbacks) override;

    virtual void OnJobCompleted(const TJobPtr& job) override;
    virtual void OnJobAborted(const TJobPtr& job) override;
    virtual void OnJobFailed(const TJobPtr& job) override;

private:
    struct TPerMediumChunkStatistics
    {
        TPerMediumChunkStatistics();

        EChunkStatus Status;

        //! Number of active replicas, per each replica index.
        int ReplicaCount[NChunkClient::ChunkReplicaIndexBound];

        //! Number of decommissioned replicas, per each replica index.
        int DecommissionedReplicaCount[NChunkClient::ChunkReplicaIndexBound];

        //! Indexes of replicas whose replication is advised.
        SmallVector<int, TypicalReplicaCount> ReplicationIndexes;

        //! Decommissioned replicas whose removal is advised.
        // NB: there's no actual need to have medium index in context of this
        // per-medium class. This is just for convenience.
        SmallVector<TNodePtrWithIndexes, TypicalReplicaCount> DecommissionedRemovalReplicas;
         //! Indexes of replicas whose removal is advised for balancing.
        SmallVector<int, TypicalReplicaCount> BalancingRemovalIndexes;
    };

    struct TChunkStatistics
    {
        TMediumMap<TPerMediumChunkStatistics> PerMediumStatistics;
        // Aggregate status across all media. The only applicable statuses are:
        //   Lost - the chunk is lost on all media;
        //   DataMissing - the chunk is missing data parts on all media;
        //   ParityMissing - the chunk is missing parity parts on all media;
        //   Precarious - the chunk has no replicas on a non-transient media;
        //   MediumWiseLost - the chunk is lost on some media, but not on others.
        ECrossMediumChunkStatus Status = ECrossMediumChunkStatus::None;
    };

    // This is for a simple optimization: updating adjacent chunks in the
    // requisition update queue is likely to produce identical results.
    struct TChunkRequisitionCache
    {
        TChunk::TParents LastChunkParents;
        std::optional<TChunkRequisition> LastChunkUpdatedRequisition;
        std::optional<TChunkRequisition> LastErasureChunkUpdatedRequisition;
    };

    TChunkRequisitionCache ChunkRequisitionCache_;

    TEphemeralRequisitionRegistry TmpRequisitionRegistry_;

    const TChunkManagerConfigPtr Config_;
    NCellMaster::TBootstrap* const Bootstrap_;
    const TChunkPlacementPtr ChunkPlacement_;
    const TJobRegistryPtr JobRegistry_;

    const NConcurrency::TPeriodicExecutorPtr RefreshExecutor_;
    const std::unique_ptr<TChunkScanner> BlobRefreshScanner_;
    const std::unique_ptr<TChunkScanner> JournalRefreshScanner_;

    const NConcurrency::TPeriodicExecutorPtr RequisitionUpdateExecutor_;
    const std::unique_ptr<TChunkScanner> BlobRequisitionUpdateScanner_;
    const std::unique_ptr<TChunkScanner> JournalRequisitionUpdateScanner_;

    const NConcurrency::TPeriodicExecutorPtr FinishedRequisitionTraverseFlushExecutor_;

    // Contains the chunk list ids for which requisition update traversals
    // have finished. These confirmations are batched and then flushed.
    std::vector<TChunkListId> ChunkListIdsWithFinishedRequisitionTraverse_;

    //! A queue of chunks to be repaired on each medium.
    //! Replica index is always GenericChunkReplicaIndex.
    //! Medium index designates the medium where the chunk is missing some of
    //! its parts. It's always equal to the index of its queue.
    //! In each queue, a single chunk may only appear once.
    std::array<TChunkRepairQueue, MaxMediumCount>  MissingPartChunkRepairQueues_ = {};
    std::array<TChunkRepairQueue, MaxMediumCount>  DecommissionedPartChunkRepairQueues_ = {};
    TDecayingMaxMinBalancer<int, double> MissingPartChunkRepairQueueBalancer_;
    TDecayingMaxMinBalancer<int, double> DecommissionedPartChunkRepairQueueBalancer_;

    const NConcurrency::TPeriodicExecutorPtr EnabledCheckExecutor_;

    const TCallback<void(NCellMaster::TDynamicClusterConfigPtr)> DynamicConfigChangedCallback_ =
        BIND(&TChunkReplicator::OnDynamicConfigChanged, MakeWeak(this));

    std::vector<TChunkId> ChunkIdsPendingEndorsementRegistration_;

    std::optional<bool> Enabled_;

    bool TryScheduleReplicationJob(
        IJobSchedulingContext* context,
        TChunkPtrWithIndexes chunkWithIndex,
        TMedium* targetMedium);
    bool TryScheduleBalancingJob(
        IJobSchedulingContext* context,
        TChunkPtrWithIndexes chunkWithIndex,
        double maxFillCoeff);
    bool TryScheduleRemovalJob(
        IJobSchedulingContext* context,
        const NChunkClient::TChunkIdWithIndexes& chunkIdWithIndex);
    bool TryScheduleRepairJob(
        IJobSchedulingContext* context,
        EChunkRepairQueue repairQueue,
        TChunkPtrWithIndexes chunkWithIndexes);

    void OnRefresh();
    void RefreshChunk(TChunk* chunk);

    void ResetChunkStatus(TChunk* chunk);
    void RemoveChunkFromQueuesOnRefresh(TChunk* chunk);
    void RemoveChunkFromQueuesOnDestroy(TChunk* chunk);

    void MaybeRememberPartMissingChunk(TChunk* chunk);

    TChunkStatistics ComputeChunkStatistics(const TChunk* chunk);

    TChunkStatistics ComputeRegularChunkStatistics(const TChunk* chunk);
    void ComputeRegularChunkStatisticsForMedium(
        TPerMediumChunkStatistics& result,
        const TChunk* chunk,
        TReplicationPolicy replicationPolicy,
        int replicaCount,
        int decommissionedReplicaCount,
        const TNodePtrWithIndexesList& decommissionedReplicas,
        bool hasSealedReplica,
        bool totallySealed,
        bool hasUnsafelyPlacedReplica);
    void ComputeRegularChunkStatisticsCrossMedia(
        TChunkStatistics& result,
        const TChunk* chunk,
        int totalReplicaCount,
        int totalDecommissionedReplicaCount,
        bool hasSealedReplicas,
        bool precarious,
        bool allMediaTransient,
        const SmallVector<int, MaxMediumCount>& mediaOnWhichLost,
        bool hasMediumOnWhichPresent,
        bool hasMediumOnWhichUnderreplicated,
        bool hasMediumOnWhichSealedMissing);

    TChunkStatistics ComputeErasureChunkStatistics(const TChunk* chunk);
    void ComputeErasureChunkStatisticsForMedium(
        TPerMediumChunkStatistics& result,
        NErasure::ICodec* codec,
        TReplicationPolicy replicationPolicy,
        std::array<TNodePtrWithIndexesList, NChunkClient::ChunkReplicaIndexBound>& decommissionedReplicas,
        int unsafelyPlacedSealedReplicaIndex,
        NErasure::TPartIndexSet& erasedIndexes,
        bool totallySealed);
    void ComputeErasureChunkStatisticsCrossMedia(
        TChunkStatistics& result,
        const TChunk* chunk,
        NErasure::ICodec* codec,
        bool allMediaTransient,
        bool allMediaDataPartsOnly,
        const TMediumMap<NErasure::TPartIndexSet>& mediumToErasedIndexes,
        const TMediumSet& activeMedia,
        const NErasure::TPartIndexSet& replicaIndexes,
        bool totallySealed);

    bool IsReplicaDecommissioned(TNodePtrWithIndexes replica);

    //! Same as corresponding #TChunk method but
    //!   - replication factors are capped by medium-specific bounds;
    //!   - additional entries may be introduced if the chunk has replicas
    //!     stored on a medium it's not supposed to have replicas on.
    TChunkReplication GetChunkAggregatedReplication(const TChunk* chunk);

    //! Same as corresponding #TChunk method but the result is capped by the medium-specific bound.
    int GetChunkAggregatedReplicationFactor(const TChunk* chunk, int mediumIndex);

    void OnRequisitionUpdate();
    void ComputeChunkRequisitionUpdate(TChunk* chunk, NProto::TReqUpdateChunkRequisition* request);

    void ClearChunkRequisitionCache();
    bool CanServeRequisitionFromCache(const TChunk* chunk);
    TChunkRequisition GetRequisitionFromCache(const TChunk* chunk);
    void CacheRequisition(const TChunk* chunk, const TChunkRequisition& requisition);

    //! Computes the actual requisition the chunk must have.
    TChunkRequisition ComputeChunkRequisition(const TChunk* chunk);

    void ConfirmChunkListRequisitionTraverseFinished(TChunkList* chunkList);
    void OnFinishedRequisitionTraverseFlush();

    //! Follows upward parent links.
    //! Stops when some owning nodes are discovered or parents become ambiguous.
    TChunkList* FollowParentLinks(TChunkList* chunkList);

    void AddToChunkRepairQueue(TChunkPtrWithIndexes chunkWithIndexes, EChunkRepairQueue queue);
    void RemoveFromChunkRepairQueues(TChunkPtrWithIndexes chunkWithIndexes);

    void FlushEndorsementQueue();

    void OnCheckEnabled();
    void OnCheckEnabledPrimary();
    void OnCheckEnabledSecondary();

    void TryRescheduleChunkRemoval(const TJobPtr& unsucceededJob);

    TChunkRequisitionRegistry* GetChunkRequisitionRegistry();

    const std::unique_ptr<TChunkScanner>& GetChunkRefreshScanner(TChunk* chunk) const;
    const std::unique_ptr<TChunkScanner>& GetChunkRequisitionUpdateScanner(TChunk* chunk) const;

    TChunkRepairQueue& ChunkRepairQueue(int mediumIndex, EChunkRepairQueue queue);
    std::array<TChunkRepairQueue, MaxMediumCount>& ChunkRepairQueues(EChunkRepairQueue queue);
    TDecayingMaxMinBalancer<int, double>& ChunkRepairQueueBalancer(EChunkRepairQueue queue);

    const TDynamicChunkManagerConfigPtr& GetDynamicConfig();
    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr /*oldConfig*/ = nullptr);
};

DEFINE_REFCOUNTED_TYPE(TChunkReplicator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
