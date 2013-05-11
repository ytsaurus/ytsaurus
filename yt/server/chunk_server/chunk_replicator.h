#pragma once

#include "public.h"
#include "chunk_replica.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/nullable.h>
#include <ytlib/misc/periodic_invoker.h>
#include <ytlib/misc/error.h>

#include <ytlib/erasure/public.h>

#include <ytlib/profiling/timing.h>

#include <ytlib/node_tracker_client/node_tracker_service.pb.h>

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

    void Initialize();
    void Finalize();

    void OnNodeRegistered(TNode* node);
    void OnNodeUnregistered(TNode* node);

    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunk*>, LostChunks);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunk*>, LostVitalChunks);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunk*>, UnderreplicatedChunks);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunk*>, OverreplicatedChunks);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunk*>, DataMissingChunks);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunk*>, ParityMissingChunks);

    void OnChunkDestroyed(TChunk* chunk);

    void ScheduleChunkRefresh(const TChunkId& chunkId);
    void ScheduleChunkRefresh(TChunk* chunk);

    void ScheduleNodeRefresh(TNode* node);

    void ScheduleUnknownChunkRemoval(TNode* node, const TChunkId& chunkdId);
    void ScheduleChunkRemoval(TNode* node, TChunkPtrWithIndex chunkWithIndex);

    void ScheduleRFUpdate(TChunkTree* chunkTree);
    void ScheduleRFUpdate(TChunk* chunk);
    void ScheduleRFUpdate(TChunkList* chunkList);

    void TouchChunk(TChunk* chunk);

    TJobPtr FindJob(const TJobId& id);
    TJobListPtr FindJobList(const TChunkId& id);

    EChunkStatus ComputeChunkStatus(TChunk* chunk);

    void ScheduleJobs(
        TNode* node,
        const std::vector<TJobPtr>& currentJobs,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort,
        std::vector<TJobPtr>* jobsToRemove);

    bool IsEnabled();

    int GetRefreshListSize() const;
    int GetRFUpdateListSize() const;

private:
    struct TChunkStatistics
    {
        TChunkStatistics();

        EChunkStatus Status;
        int ReplicaCount[NErasure::MaxTotalPartCount];
        int DecommissionedReplicaCount[NErasure::MaxTotalPartCount];
    };

    struct TRefreshEntry
    {
        TRefreshEntry();

        TChunk* Chunk;
        NProfiling::TCpuInstant When;
    };

    TChunkManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;
    TChunkPlacementPtr ChunkPlacement;

    NProfiling::TCpuDuration ChunkRefreshDelay;
    TNullable<bool> LastEnabled;

    TPeriodicInvokerPtr RefreshInvoker;
    std::deque<TRefreshEntry> RefreshList;

    TPeriodicInvokerPtr RFUpdateInvoker;
    std::deque<TChunk*> RFUpdateList;

    yhash_map<TJobId, TJobPtr> JobMap;
    //! Keyed by whole (not encoded) chunk ids.
    yhash_map<TChunkId, TJobListPtr> JobListMap;

    TChunkRepairQueue RepairQueue;

    void ProcessExistingJobs(
        TNode* node,
        const std::vector<TJobPtr>& currentJobs,
        std::vector<TJobPtr>* jobsToAbort,
        std::vector<TJobPtr>* jobsToRemove);

    DECLARE_FLAGGED_ENUM(EJobScheduleFlags,
        ((None)     (0x0000))
        ((Scheduled)(0x0001))
        ((Purged)   (0x0002))
    );

    EJobScheduleFlags ScheduleReplicationJob(
        TNode* sourceNode,
        const TChunkId& chunkId,
        TJobPtr* job);
    EJobScheduleFlags ScheduleBalancingJob(
        TNode* sourceNode,
        TChunkPtrWithIndex chunkWithIndex,
        double maxFillCoeff,
        TJobPtr* jobsToStart);
    EJobScheduleFlags ScheduleRemovalJob(
        TNode* node,
        const TChunkId& chunkId,
        TJobPtr* job);
    EJobScheduleFlags ScheduleRepairJob(
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

    TChunkStatistics ComputeChunkStatistics(TChunk* chunk);
    TChunkStatistics ComputeRegularChunkStatistics(TChunk* chunk);
    TChunkStatistics ComputeErasureChunkStatistics(TChunk* chunk);

    bool IsReplicaDecommissioned(TNodePtrWithIndex replica);

    bool HasRunningJobs(const TChunkId& chunkId);

    void OnRFUpdate();
    void OnRFUpdateCommitSucceeded();
    void OnRFUpdateCommitFailed(const TError& error);

    //! Computes the actual replication factor the chunk must have.
    int ComputeReplicationFactor(TChunk* chunk);

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

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
