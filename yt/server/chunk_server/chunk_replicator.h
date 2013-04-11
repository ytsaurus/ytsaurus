#pragma once

#include "public.h"
#include "chunk_replica.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/nullable.h>
#include <ytlib/misc/periodic_invoker.h>
#include <ytlib/misc/error.h>

#include <ytlib/profiling/timing.h>

#include <ytlib/node_tracker_client/node_tracker_service.pb.h>

#include <server/cell_master/public.h>

#include <deque>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TReplicaStatistics
{
    int ReplicationFactor;
    int StoredCount;
    int CachedCount;
    int PlusCount;
    int MinusCount;
};

Stroka ToString(const TReplicaStatistics& statistics);

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

    void OnNodeRegistered(TNode* node);
    void OnNodeUnregistered(TNode* node);

    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunk*>, LostChunks);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunk*>, LostVitalChunks);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunk*>, UnderreplicatedChunks);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunk*>, OverreplicatedChunks);

    void OnChunkRemoved(TChunk* chunk);

    void ScheduleChunkRefresh(const TChunkId& chunkId);
    void ScheduleChunkRefresh(TChunk* chunk);

    void ScheduleChunkRemoval(TNode* node, const TChunkId& chunkdId);
    void ScheduleChunkRemoval(TNode* node, TChunkPtrWithIndex chunkWithIndex);

    void ScheduleRFUpdate(TChunkTree* chunkTree);
    void ScheduleRFUpdate(TChunk* chunk);
    void ScheduleRFUpdate(TChunkList* chunkList);

    TJobPtr FindJob(const TJobId& id);
    TJobListPtr FindJobList(const TChunkId& id);

    void ScheduleJobs(
        TNode* node,
        const std::vector<TJobPtr>& currentJobs,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToStop);

    bool IsEnabled();

    int GetRefreshListSize() const;
    int GetRFUpdateListSize() const;

private:
    TChunkManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;
    TChunkPlacementPtr ChunkPlacement;

    NProfiling::TCpuDuration ChunkRefreshDelay;
    TNullable<bool> LastEnabled;

    struct TRefreshEntry
    {
        TChunk* Chunk;
        NProfiling::TCpuInstant When;
    };

    TPeriodicInvokerPtr RefreshInvoker;
    std::deque<TRefreshEntry> RefreshList;

    TPeriodicInvokerPtr RFUpdateInvoker;
    std::deque<TChunk*> RFUpdateList;

    yhash_map<TJobId, TJobPtr> JobMap;
    yhash_map<TChunkId, TJobListPtr> JobListMap;

    void ProcessExistingJobs(
        TNode* node,
        const std::vector<TJobPtr>& currentJobs,
        std::vector<TJobPtr>* jobsToStop,
        int* replicationJobCount,
        int* removalJobCount);

    DECLARE_FLAGGED_ENUM(EScheduleFlags,
        ((None)     (0x0000))
        ((Scheduled)(0x0001))
        ((Purged)   (0x0002))
    );

    EScheduleFlags ScheduleReplicationJob(
        TNode* sourceNode,
        const TChunkId& chunkId,
        std::vector<TJobPtr>* jobsToStart);
    EScheduleFlags ScheduleBalancingJob(
        TNode* sourceNode,
        TChunkPtrWithIndex chunkWithIndex,
        double maxFillCoeff,
        std::vector<TJobPtr>* jobsToStart);
    EScheduleFlags ScheduleRemovalJob(
        TNode* node,
        const TChunkId& chunkId,
        std::vector<TJobPtr>* jobsToStart);
    void ScheduleNewJobs(
        TNode* node,
        int maxReplicationJobsToStart,
        int maxRemovalJobsToStart,
        std::vector<TJobPtr>* jobsToStart);

    TReplicaStatistics GetReplicaStatistics(const TChunk* chunk);

    void OnRefresh();
    void Refresh(TChunk* chunk);
    static int ComputeReplicationPriority(const TReplicaStatistics& statistics);

    void OnRFUpdate();
    void OnRFUpdateCommitSucceeded();
    void OnRFUpdateCommitFailed(const TError& error);

    //! Computes the actual replication factor the chunk must have.
    int ComputeReplicationFactor(const TChunk* chunk);

    //! Follows upward parent links.
    //! Stops when some owning nodes are discovered or parents become ambiguous.
    TChunkList* FollowParentLinks(TChunkList* chunkList);

    void RegisterJob(TJobPtr job);
    void UnregisterJob(TJobPtr job);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
