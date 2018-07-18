#pragma once

#include "private.h"
#include "chunk.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/misc/max_min_balancer.h>

#include <yt/server/node_tracker_server/data_center.h>

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/erasure/public.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/nullable.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/small_set.h>

#include <yt/core/profiling/timing.h>

#include <functional>
#include <deque>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicator
    : public TRefCounted
{
public:
    TChunkReplicator(
        TChunkManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap,
        TChunkPlacementPtr chunkPlacement);

    void Start(TChunk* frontChunk, int chunkCount);
    void Stop();

    void OnNodeRegistered(TNode* node);
    void OnNodeUnregistered(TNode* node);
    void OnNodeDisposed(TNode* node);

    // 'On all of the media' chunk states. E.g. LostChunks contain chunks that
    // have been lost on all of the media.
    DEFINE_BYREF_RO_PROPERTY(THashSet<TChunk*>, LostChunks);
    DEFINE_BYREF_RO_PROPERTY(THashSet<TChunk*>, LostVitalChunks);
    DEFINE_BYREF_RO_PROPERTY(THashSet<TChunk*>, DataMissingChunks);
    DEFINE_BYREF_RO_PROPERTY(THashSet<TChunk*>, ParityMissingChunks);
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

    // src DC -> dst DC -> data size
    using TInterDCEdgeDataSize = THashMap<const NNodeTrackerServer::TDataCenter*, THashMap<const NNodeTrackerServer::TDataCenter*, i64>>;
    DEFINE_BYREF_RO_PROPERTY(TInterDCEdgeDataSize, InterDCEdgeConsumption);
    DEFINE_BYREF_RO_PROPERTY(TInterDCEdgeDataSize, InterDCEdgeCapacities);

    using TJobCounters = TEnumIndexedVector<int, EJobType, EJobType::ReplicatorFirst, EJobType::ReplicatorLast>;
    // Number of jobs running - per job type. For profiling.
    DEFINE_BYREF_RO_PROPERTY(TJobCounters, JobCounters);

    void OnChunkDestroyed(TChunk* chunk);
    void OnReplicaRemoved(
        TNode* node,
        TChunkPtrWithIndexes chunkWithIndexes,
        ERemoveReplicaReason reason);

    void ScheduleChunkRefresh(TChunk* chunk);
    void ScheduleNodeRefresh(TNode* node);

    void ScheduleUnknownReplicaRemoval(
        TNode* node,
        const NChunkClient::TChunkIdWithIndexes& chunkdIdWithIndexes);
    void ScheduleReplicaRemoval(
        TNode* node,
        TChunkPtrWithIndexes chunkWithIndexes);

    void ScheduleRequisitionUpdate(TChunkTree* chunkTree);
    void ScheduleRequisitionUpdate(TChunk* chunk);
    void ScheduleRequisitionUpdate(TChunkList* chunkList);

    void TouchChunk(TChunk* chunk);

    TPerMediumArray<EChunkStatus> ComputeChunkStatuses(TChunk* chunk);

    void ScheduleJobs(
        TNode* node,
        const std::vector<TJobPtr>& currentJobs,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort,
        std::vector<TJobPtr>* jobsToRemove);

    void HandleNodeDataCenterChange(TNode* node, NNodeTrackerServer::TDataCenter* oldDataCenter);

    bool IsEnabled();

    int GetRefreshQueueSize() const;
    int GetRequisitionUpdateQueueSize() const;

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
        TPerMediumArray<TPerMediumChunkStatistics> PerMediumStatistics;
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
        TNullable<TChunkRequisition> LastChunkUpdatedRequisition;
        TNullable<TChunkRequisition> LastErasureChunkUpdatedRequisition;
    };

    TChunkRequisitionCache ChunkRequisitionCache_;

    TEphemeralRequisitionRegistry TmpRequisitionRegistry_;

    const TChunkManagerConfigPtr Config_;
    NCellMaster::TBootstrap* const Bootstrap_;
    const TChunkPlacementPtr ChunkPlacement_;

    NProfiling::TCpuDuration ChunkRefreshDelay_;

    const NConcurrency::TPeriodicExecutorPtr RefreshExecutor_;
    const std::unique_ptr<TChunkScanner> RefreshScanner_;

    const NConcurrency::TPeriodicExecutorPtr RequisitionUpdateExecutor_;
    const std::unique_ptr<TChunkScanner> RequisitionUpdateScanner_;

    //! A queue of chunks to be repaired on each medium.
    //! Replica index is always GenericChunkReplicaIndex.
    //! Medium index designates the medium where the chunk is missing some of
    //! its parts. It's always equal to the index of its queue.
    //! In each queue, a single chunk may only appear once.
    TPerMediumArray<TChunkRepairQueue>  ChunkRepairQueues_ = {};
    TDecayingMaxMinBalancer<int, double> ChunkRepairQueueBalancer_;

    const NConcurrency::TPeriodicExecutorPtr EnabledCheckExecutor_;

    const NConcurrency::IThroughputThrottlerPtr JobThrottler_;

    TNullable<bool> Enabled_;

    NProfiling::TCpuInstant InterDCEdgeCapacitiesLastUpdateTime = {};
    // Cached from InterDCEdgeConsumption and InterDCEdgeCapacities.
    THashMap<const NNodeTrackerServer::TDataCenter*, SmallSet<const NNodeTrackerServer::TDataCenter*, NNodeTrackerServer::TypicalInterDCEdgeCount>> UnsaturatedInterDCEdges;

    void ProcessExistingJobs(
        TNode* node,
        const std::vector<TJobPtr>& currentJobs,
        std::vector<TJobPtr>* jobsToAbort,
        std::vector<TJobPtr>* jobsToRemove);

    TJobId GenerateJobId();
    bool CreateReplicationJob(
        TNode* sourceNode,
        TChunkPtrWithIndexes chunkWithIndex,
        TMedium* targetMedium,
        TJobPtr* job);
    bool CreateBalancingJob(
        TNode* sourceNode,
        TChunkPtrWithIndexes chunkWithIndex,
        double maxFillCoeff,
        TJobPtr* jobsToStart);
    bool CreateRemovalJob(
        TNode* node,
        const NChunkClient::TChunkIdWithIndexes& chunkIdWithIndex,
        TJobPtr* job);
    bool CreateRepairJob(
        TNode* node,
        TChunkPtrWithIndexes chunkWithIndexes,
        TJobPtr* job);
    bool CreateSealJob(
        TNode* node,
        TChunkPtrWithIndexes chunkWithIndexes,
        TJobPtr* job);
    void ScheduleNewJobs(
        TNode* node,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort);

    void OnRefresh();
    void RefreshChunk(TChunk* chunk);

    void ResetChunkStatus(TChunk* chunk);
    void RemoveChunkFromQueuesOnRefresh(TChunk* chunk);
    void RemoveChunkFromQueuesOnDestroy(TChunk* chunk);
    void CancelChunkJobs(TChunk* chunk);

    TChunkStatistics ComputeChunkStatistics(TChunk* chunk);
    TChunkStatistics ComputeRegularChunkStatistics(TChunk* chunk);
    TChunkStatistics ComputeErasureChunkStatistics(TChunk* chunk);
    TChunkStatistics ComputeJournalChunkStatistics(TChunk* chunk);

    // Implementation details for Compute{Regular,Erasure}ChunkStatistics().
    void ComputeRegularChunkStatisticsForMedium(
        TPerMediumChunkStatistics& result,
        int replicationFactor,
        int replicaCount,
        int decommissionedReplicaCount,
        const TNodePtrWithIndexesList& decommissionedReplicas,
        bool hasUnsafelyPlacedReplicas);
    void ComputeRegularChunkStatisticsCrossMedia(
        TChunkStatistics& result,
        bool precarious,
        bool allMediaTransient,
        const SmallVector<int, MaxMediumCount>& mediaOnWhichLost,
        int mediaOnWhichPresentCount);
    void ComputeErasureChunkStatisticsForMedium(
        TPerMediumChunkStatistics& result,
        NErasure::ICodec* codec,
        int replicationFactor,
        std::array<TNodePtrWithIndexesList, NChunkClient::ChunkReplicaIndexBound>& decommissionedReplicas,
        int unsafelyPlacedReplicaIndex,
        NErasure::TPartIndexSet& erasedIndexes,
        bool dataPartsOnly);
    void ComputeErasureChunkStatisticsCrossMedia(
        TChunkStatistics& result,
        NErasure::ICodec* codec,
        bool allMediaTransient,
        bool allMediaDataPartsOnly,
        const TPerMediumArray<NErasure::TPartIndexSet>& mediumToErasedIndexes,
        const TMediumSet& activeMedia);

    bool IsReplicaDecommissioned(TNodePtrWithIndexes replica);

    void OnRequisitionUpdate();
    void ComputeChunkRequisitionUpdate(TChunk* chunk, NProto::TReqUpdateChunkRequisition* request);

    void ClearChunkRequisitionCache();
    bool CanServeRequisitionFromCache(const TChunk* chunk);
    TChunkRequisition GetRequisitionFromCache(const TChunk* chunk);
    void CacheRequisition(const TChunk* chunk, const TChunkRequisition& requisition);

    //! Computes the actual requisition the chunk must have.
    TChunkRequisition ComputeChunkRequisition(const TChunk* chunk);

    //! Follows upward parent links.
    //! Stops when some owning nodes are discovered or parents become ambiguous.
    TChunkList* FollowParentLinks(TChunkList* chunkList);

    void RegisterJob(const TJobPtr& job);
    void UnregisterJob(const TJobPtr& job);

    void UpdateJobCountGauge(EJobType jobType, int delta);

    void AddToChunkRepairQueue(TChunkPtrWithIndexes chunkWithIndexes);
    void RemoveFromChunkRepairQueue(TChunkPtrWithIndexes chunkWithIndexes);

    void InitInterDCEdges();
    void UpdateInterDCEdgeCapacities();
    void UpdateUnsaturatedInterDCEdges();
    void UpdateInterDCEdgeConsumption(
        const TJobPtr& job,
        const NNodeTrackerServer::TDataCenter* srcDataCenter,
        int sizeMultiplier);
    bool HasUnsaturatedInterDCEdgeStartingFrom(const NNodeTrackerServer::TDataCenter* srcDataCenter);

    void OnCheckEnabled();
    void OnCheckEnabledPrimary();
    void OnCheckEnabledSecondary();

    TChunkRequisitionRegistry* GetChunkRequisitionRegistry();
};

DEFINE_REFCOUNTED_TYPE(TChunkReplicator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
