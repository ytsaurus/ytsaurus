#pragma once

#include "public.h"

#include <server/cell_master/public.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/property.h>
#include <ytlib/misc/nullable.h>
#include <ytlib/profiling/public.h>
#include <server/chunk_server/chunk_service.pb.h>

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
        TChunkPlacementPtr chunkPlacement,
        TNodeLeaseTrackerPtr nodeLeaseTracker);

    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunkId>, LostChunkIds);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunkId>, UnderreplicatedChunkIds);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunkId>, OverreplicatedChunkIds);

    void OnNodeRegistered(const TDataNode* node);
    void OnNodeUnregistered(const TDataNode* node);

    void OnChunkRemoved(const TChunk* chunk);

    void RefreshAllChunks();

    void ScheduleChunkRefresh(const TChunkId& chunkId);

    void ScheduleChunkRemoval(const TDataNode* node, const TChunkId& chunkId);

    void ScheduleJobs(
        TDataNode* node,
        const std::vector<NProto::TJobInfo>& runningJobs,
        std::vector<NProto::TJobStartInfo>* jobsToStart,
        std::vector<NProto::TJobStopInfo>* jobsToStop);

    bool IsEnabled();

private:
    TChunkManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;
    TChunkPlacementPtr ChunkPlacement;
    TNodeLeaseTrackerPtr NodeLeaseTracker;

    NProfiling::TCpuDuration ChunkRefreshDelay;
    TNullable<bool> LastEnabled;

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

    struct TRefreshEntry
    {
        TChunkId ChunkId;
        NProfiling::TCpuInstant When;
    };

    yhash_set<TChunkId> RefreshSet;
    std::deque<TRefreshEntry> RefreshList;

    struct TNodeInfo
    {
        typedef yhash_set<TChunkId> TChunkIds;
        TChunkIds ChunksToReplicate;
        TChunkIds ChunksToRemove;
    };

    typedef yhash_map<TNodeId, TNodeInfo> TNodeInfoMap;
    TNodeInfoMap NodeInfoMap;

    TNodeInfo* FindNodeInfo(TNodeId nodeId);
    TNodeInfo* GetNodeInfo(TNodeId nodeId);

    void ProcessExistingJobs(
        const TDataNode* node,
        const std::vector<NProto::TJobInfo>& runningJobs,
        std::vector<NProto::TJobStopInfo>* jobsToStop,
        int* replicationJobCount,
        int* removalJobCount);

    bool IsRefreshScheduled(const TChunkId& chunkId);

    DECLARE_ENUM(EScheduleFlags,
        ((None)(0x0000))
        ((Scheduled)(0x0001))
        ((Purged)(0x0002))
    );

    EScheduleFlags ScheduleReplicationJob(
        TDataNode* sourceNode,
        const TChunkId& chunkId,
        std::vector<NProto::TJobStartInfo>* jobsToStart);
    EScheduleFlags ScheduleBalancingJob(
        TDataNode* sourceNode,
        TChunk* chunk,
        std::vector<NProto::TJobStartInfo>* jobsToStart);
    EScheduleFlags ScheduleRemovalJob(
        TDataNode* node,
        const TChunkId& chunkId,
        std::vector<NProto::TJobStartInfo>* jobsToStart);
    void ScheduleNewJobs(
        TDataNode* node,
        int maxReplicationJobsToStart,
        int maxRemovalJobsToStart,
        std::vector<NProto::TJobStartInfo>* jobsToStart);

    void Refresh(const TChunk* chunk);
    int GetReplicationFactor(const TChunk* chunk);

    struct TReplicaStatistics
    {
        int ReplicationFactor;
        int StoredCount;
        int CachedCount;
        int PlusCount;
        int MinusCount;
    };

    TReplicaStatistics GetReplicaStatistics(const TChunk* chunk);
    static Stroka ToString(const TReplicaStatistics& statistics);

    void ScheduleNextRefresh();
    void OnRefresh();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
