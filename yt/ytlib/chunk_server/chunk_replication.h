#pragma once

#include "common.h"
#include "chunk_manager.h"
#include "chunk_placement.h"

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
    typedef NProto::TReqHolderHeartbeat::TJobInfo TJobInfo;
    typedef NProto::TRspHolderHeartbeat::TJobStartInfo TJobStartInfo;
    
    TChunkReplication(
        TChunkManager* chunkManager,
        TChunkPlacement* chunkPlacement,
        TChunkManager::TConfig* config,
        IInvoker* invoker);

    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunkId>, LostChunkIds);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunkId>, UnderreplicatedChunkIds);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TChunkId>, OverreplicatedChunkIds);

    void OnHolderRegistered(const THolder& holder);
    void OnHolderUnregistered(const THolder& holder);

    void ScheduleChunkRefresh(const TChunkId& chunkId);

    void ScheduleChunkRemoval(const THolder& holder, const TChunkId& chunkId);

    void RunJobControl(
        const THolder& holder,
        const yvector<TJobInfo>& runningJobs,
        yvector<TJobStartInfo>* jobsToStart,
        yvector<TJobId>* jobsToStop);

private:
    TChunkManager::TPtr ChunkManager;
    TChunkManager::TConfig::TPtr Config;
    TChunkPlacement::TPtr ChunkPlacement;

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

    struct TRefreshEntry
    {
        TChunkId ChunkId;
        TInstant When;
    };

    IInvoker::TPtr Invoker;
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
        const yvector<TJobInfo>& runningJobs,
        yvector<TJobId>* jobsToStop,
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
        yvector<TJobStartInfo>* jobsToStart);
    EScheduleFlags ScheduleBalancingJob(
        const THolder& sourceHolder,
        const TChunkId& chunkId,
        yvector<TJobStartInfo>* jobsToStart);
    EScheduleFlags ScheduleRemovalJob(
        const THolder& holder,
        const TChunkId& chunkId,
        yvector<TJobStartInfo>* jobsToStart);
    void ScheduleJobs(
        const THolder& holder,
        int maxReplicationJobsToStart,
        int maxRemovalJobsToStart,
        yvector<TJobStartInfo>* jobsToStart);

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
