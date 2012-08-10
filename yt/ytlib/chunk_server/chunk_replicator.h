#pragma once

#include "public.h"

#include <ytlib/cell_master/public.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/property.h>
#include <ytlib/misc/nullable.h>
#include <ytlib/profiling/public.h>
#include <ytlib/chunk_server/chunk_service.pb.h>

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
        THolderLeaseTrackerPtr holderLeaseTracker);

    DEFINE_BYREF_RO_PROPERTY(std::unordered_set<TChunkId>, LostChunkIds);
    DEFINE_BYREF_RO_PROPERTY(std::unordered_set<TChunkId>, UnderreplicatedChunkIds);
    DEFINE_BYREF_RO_PROPERTY(std::unordered_set<TChunkId>, OverreplicatedChunkIds);

    void OnHolderRegistered(const THolder* holder);
    void OnHolderUnregistered(const THolder* holder);

    void OnChunkRemoved(const TChunk* chunk);

    void RefreshAllChunks();

    void ScheduleChunkRefresh(const TChunkId& chunkId);

    void ScheduleChunkRemoval(const THolder* holder, const TChunkId& chunkId);

    void ScheduleJobs(
        THolder* holder,
        const std::vector<NProto::TJobInfo>& runningJobs,
        std::vector<NProto::TJobStartInfo>* jobsToStart,
        std::vector<NProto::TJobStopInfo>* jobsToStop);

    bool IsEnabled();

private:
    TChunkManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;
    TChunkPlacementPtr ChunkPlacement;
    THolderLeaseTrackerPtr HolderLeaseTracker;

    NProfiling::TCpuDuration ChunkRefreshDelay;
    TNullable<bool> LastEnabled;

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

    struct TRefreshEntry
    {
        TChunkId ChunkId;
        NProfiling::TCpuInstant When;
    };

    std::unordered_set<TChunkId> RefreshSet;
    std::deque<TRefreshEntry> RefreshList;

    struct THolderInfo
    {
        typedef std::unordered_set<TChunkId> TChunkIds;
        TChunkIds ChunksToReplicate;
        TChunkIds ChunksToRemove;
    };

    typedef std::unordered_map<THolderId, THolderInfo> THolderInfoMap;
    THolderInfoMap HolderInfoMap;

    THolderInfo* FindHolderInfo(THolderId holderId);
    THolderInfo* GetHolderInfo(THolderId holderId);

    void ProcessExistingJobs(
        const THolder* holder,
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
        THolder* sourceHolder,
        const TChunkId& chunkId,
        std::vector<NProto::TJobStartInfo>* jobsToStart);
    EScheduleFlags ScheduleBalancingJob(
        THolder* sourceHolder,
        TChunk* chunk,
        std::vector<NProto::TJobStartInfo>* jobsToStart);
    EScheduleFlags ScheduleRemovalJob(
        THolder* holder,
        const TChunkId& chunkId,
        std::vector<NProto::TJobStartInfo>* jobsToStart);
    void ScheduleNewJobs(
        THolder* holder,
        int maxReplicationJobsToStart,
        int maxRemovalJobsToStart,
        std::vector<NProto::TJobStartInfo>* jobsToStart);

    void Refresh(const TChunk* chunk);
    int GetReplicationFactor(const TChunk* chunk);
    void GetReplicaStatistics(
        const TChunk* chunk,
        int* desiredCount,
        int* storedCount,
        int* cachedCount,
        int* plusCount,
        int* minusCount);
    void ScheduleNextRefresh();
    void OnRefresh();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
