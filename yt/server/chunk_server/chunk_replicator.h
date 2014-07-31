#pragma once

#include "public.h"
#include "chunk.h"
#include "chunk_replica.h"

#include <core/misc/property.h>
#include <core/misc/nullable.h>
#include <core/concurrency/periodic_executor.h>
#include <core/misc/error.h>

#include <core/erasure/public.h>

#include <core/profiling/timing.h>

#include <ytlib/node_tracker_client/node_tracker_service.pb.h>

#include <ytlib/chunk_client/chunk_replica.h>

#include <server/cell_master/public.h>

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

    void Start();
    void Stop();

    void OnNodeRegistered(TNode* node);
    void OnNodeUnregistered(TNode* node);

    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunk*>, LostChunks);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunk*>, LostVitalChunks);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunk*>, UnderreplicatedChunks);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunk*>, OverreplicatedChunks);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunk*>, DataMissingChunks);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunk*>, ParityMissingChunks);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunk*>, QuorumMissingChunks);

    void OnChunkDestroyed(TChunk* chunk);

    void ScheduleChunkRefresh(const TChunkId& chunkId);
    void ScheduleChunkRefresh(TChunk* chunk);

    void ScheduleNodeRefresh(TNode* node);

    void ScheduleUnknownChunkRemoval(TNode* node, const NChunkClient::TChunkIdWithIndex& chunkdIdWithIndex);
    void ScheduleChunkRemoval(TNode* node, TChunkPtrWithIndex chunkWithIndex);

    void SchedulePropertiesUpdate(TChunkTree* chunkTree);
    void SchedulePropertiesUpdate(TChunk* chunk);
    void SchedulePropertiesUpdate(TChunkList* chunkList);

    void TouchChunk(TChunk* chunk);

    TJobPtr FindJob(const TJobId& id);
    TJobListPtr FindJobList(TChunk* chunk);

    EChunkStatus ComputeChunkStatus(TChunk* chunk);

    void ScheduleJobs(
        TNode* node,
        const std::vector<TJobPtr>& currentJobs,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort,
        std::vector<TJobPtr>* jobsToRemove);

    bool IsEnabled();

    int GetRefreshListSize() const;
    int GetPropertiesUpdateListSize() const;

private:
    struct TJobRequest
    {
        TJobRequest(int index, int count);

        //! Replica index the request applies to.
        int Index;

        //! Number of replicas to create/remove.
        int Count;
    };

    struct TChunkStatistics
    {
        TChunkStatistics();

        EChunkStatus Status;

        //! Number of active replicas, per each replica index.
        int ReplicaCount[NChunkClient::ChunkReplicaIndexBound];
        
        //! Number of decommissioned replicas, per each replica index.
        int DecommissionedReplicaCount[NChunkClient::ChunkReplicaIndexBound];

        //! Recommended replications.
        SmallVector<TJobRequest, TypicalReplicaCount> ReplicationRequests;
        
        //! Recommended removals of decommissioned replicas. 
        TNodePtrWithIndexList DecommissionedRemovalRequests;

        //! Recommended removals to active replicas.
        //! Removal targets must be selected among most loaded nodes.
        //! This can only be nonempty if |DecommissionedRemovalRequests| is empty.
        SmallVector<TJobRequest, TypicalReplicaCount> BalancingRemovalRequests;
        
    };

    struct TRefreshEntry
    {
        TChunk* Chunk = nullptr;
        NProfiling::TCpuInstant When;
    };

    TChunkManagerConfigPtr Config_;
    NCellMaster::TBootstrap* Bootstrap_;
    TChunkPlacementPtr ChunkPlacement_;

    NProfiling::TCpuDuration ChunkRefreshDelay_;
    TNullable<bool> LastEnabled_;

    NConcurrency::TPeriodicExecutorPtr RefreshExecutor_;
    std::deque<TRefreshEntry> RefreshList_;

    NConcurrency::TPeriodicExecutorPtr PropertiesUpdateExecutor_;
    std::deque<TChunk*> PropertiesUpdateList_;

    yhash_map<TJobId, TJobPtr> JobMap_;
    yhash_map<TChunk*, TJobListPtr> JobListMap_;

    TChunkRepairQueue ChunkRepairQueue_;


    void ProcessExistingJobs(
        TNode* node,
        const std::vector<TJobPtr>& currentJobs,
        std::vector<TJobPtr>* jobsToAbort,
        std::vector<TJobPtr>* jobsToRemove);

    bool CreateReplicationJob(
        TNode* sourceNode,
        TChunkPtrWithIndex chunkWithIndex,
        TJobPtr* job);
    bool CreateBalancingJob(
        TNode* sourceNode,
        TChunkPtrWithIndex chunkWithIndex,
        double maxFillCoeff,
        TJobPtr* jobsToStart);
    bool CreateRemovalJob(
        TNode* node,
        const NChunkClient::TChunkIdWithIndex& chunkIdWithIndex,
        TJobPtr* job);
    bool CreateRepairJob(
        TNode* node,
        TChunk* chunk,
        TJobPtr* job);
    bool CreateSealJob(
        TNode* node,
        TChunk* chunk,
        TJobPtr* job);
    void ScheduleNewJobs(
        TNode* node,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort);

    void OnRefresh();
    void RefreshChunk(TChunk* chunk);

    void ResetChunkStatus(TChunk* chunk);
    void ResetChunkJobs(TChunk* chunk);

    TChunkStatistics ComputeChunkStatistics(TChunk* chunk);
    TChunkStatistics ComputeRegularChunkStatistics(TChunk* chunk);
    TChunkStatistics ComputeErasureChunkStatistics(TChunk* chunk);
    TChunkStatistics ComputeJournalChunkStatistics(TChunk* chunk);

    bool IsReplicaDecommissioned(TNodePtrWithIndex replica);

    bool HasRunningJobs(TChunk* chunk);
    bool HasRunningJobs(TChunkPtrWithIndex replica);

    void OnPropertiesUpdate();

    //! Computes the actual properties the chunk must have.
    TChunkProperties ComputeChunkProperties(TChunk* chunk);

    //! Follows upward parent links.
    //! Stops when some owning nodes are discovered or parents become ambiguous.
    TChunkList* FollowParentLinks(TChunkList* chunkList);

    void RegisterJob(TJobPtr job);

    DECLARE_FLAGGED_ENUM(EJobUnregisterFlags,
        ((None)                  (0x0000))
        ((UnregisterFromChunk)   (0x0001))
        ((UnregisterFromNode)    (0x0002))
        ((ScheduleChunkRefresh)  (0x0004))
        ((All)                   (0xffff))
    );
    void UnregisterJob(TJobPtr job, EJobUnregisterFlags flags = EJobUnregisterFlags::All);

    void AddToChunkRepairQueue(TChunk* chunk);
    void RemoveFromChunkRepairQueue(TChunk* chunk);

};

DEFINE_REFCOUNTED_TYPE(TChunkReplicator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
