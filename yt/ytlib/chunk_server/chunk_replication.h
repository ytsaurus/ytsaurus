#pragma once

#include "config.h"
#include "chunk_placement.h"

#include <ytlib/cell_master/public.h>
#include <ytlib/misc/thread_affinity.h>

#include <util/generic/deque.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkReplication
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TChunkReplication> TPtr;
    typedef TChunkManagerConfig TConfig;
    
    TChunkReplication(
        TConfig* config,
        NCellMaster::TBootstrap* bootstrap,
        TChunkPlacement* chunkPlacement);

    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunkId>, LostChunkIds);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunkId>, UnderreplicatedChunkIds);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunkId>, OverreplicatedChunkIds);

    void OnHolderRegistered(const THolder& holder);
    void OnHolderUnregistered(const THolder& holder);

    void RefreshAllChunks();

    void ScheduleChunkRefresh(const TChunkId& chunkId);

    void ScheduleChunkRemoval(const THolder& holder, const TChunkId& chunkId);

    void RunJobControl(
        const THolder& holder,
        const yvector<NProto::TJobInfo>& runningJobs,
        yvector<NProto::TJobStartInfo>* jobsToStart,
        yvector<NProto::TJobStopInfo>* jobsToStop);

private:
    TConfig::TPtr Config;
    NCellMaster::TBootstrap* Bootstrap;
    TChunkPlacement::TPtr ChunkPlacement;

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

    struct TRefreshEntry
    {
        TChunkId ChunkId;
        TInstant When;
    };

    yhash_set<TChunkId> RefreshSet;
    ydeque<TRefreshEntry> RefreshList;

    struct THolderInfo
    {
        typedef yhash_set<TChunkId> TChunkIds;
        TChunkIds ChunksToReplicate;
        TChunkIds ChunksToRemove;
    };

    typedef yhash_map<THolderId, THolderInfo> THolderInfoMap;
    THolderInfoMap HolderInfoMap;

    THolderInfo* FindHolderInfo(THolderId holderId);
    THolderInfo& GetHolderInfo(THolderId holderId);

    void ProcessExistingJobs(
        const THolder& holder,
        const yvector<NProto::TJobInfo>& runningJobs,
        yvector<NProto::TJobStopInfo>* jobsToStop,
        int* replicationJobCount,
        int* removalJobCount);

    bool IsRefreshScheduled(const TChunkId& chunkId);

    DECLARE_ENUM(EScheduleFlags,
        ((None)(0x0000))
        ((Scheduled)(0x0001))
        ((Purged)(0x0002))
    );

    EScheduleFlags ScheduleReplicationJob(
        const THolder& sourceHolder,
        const TChunkId& chunkId,
        yvector<NProto::TJobStartInfo>* jobsToStart);
    EScheduleFlags ScheduleBalancingJob(
        const THolder& sourceHolder,
        const TChunkId& chunkId,
        yvector<NProto::TJobStartInfo>* jobsToStart);
    EScheduleFlags ScheduleRemovalJob(
        const THolder& holder,
        const TChunkId& chunkId,
        yvector<NProto::TJobStartInfo>* jobsToStart);
    void ScheduleJobs(
        const THolder& holder,
        int maxReplicationJobsToStart,
        int maxRemovalJobsToStart,
        yvector<NProto::TJobStartInfo>* jobsToStart);

    void Refresh(const TChunk& chunk);
    int GetDesiredReplicaCount(const TChunk& chunk);
    void GetReplicaStatistics(
        const TChunk& chunk,
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
