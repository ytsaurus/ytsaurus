#pragma once

#include "common.h"
#include "chunk_manager.h"
#include "chunk_placement.h"

#include "../misc/thread_affinity.h"

#include <util/generic/deque.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkReplication
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunkReplication> TPtr;

    TChunkReplication(
        TChunkManager* chunkManager,
        TChunkPlacement* chunkPlacement,
        IInvoker* invoker);

    void OnHolderRegistered(const THolder& holder);
    void OnHolderUnregistered(const THolder& holder);

    void OnReplicaAdded(const THolder& holder, const TChunk& chunk);
    void OnReplicaRemoved(const THolder& holder, const TChunk& chunk);

    void ScheduleChunkRemoval(const THolder& holder, const NChunkClient::TChunkId& chunkId);

    void RunJobControl(
        const THolder& holder,
        const yvector<NProto::TJobInfo>& runningJobs,
        yvector<NProto::TJobStartInfo>* jobsToStart,
        yvector<TJobId>* jobsToStop);

private:
    TChunkManager::TPtr ChunkManager;
    TChunkPlacement::TPtr ChunkPlacement;

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

    struct TRefreshEntry
    {
        NChunkClient::TChunkId ChunkId;
        TInstant When;
    };

    IInvoker::TPtr Invoker;
    yhash_set<NChunkClient::TChunkId> RefreshSet;
    ydeque<TRefreshEntry> RefreshList;

    struct THolderInfo
    {
        typedef yhash_set<NChunkClient::TChunkId> TChunkIds;
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
        yvector<TJobId>* jobsToStop,
        int* replicationJobCount,
        int* removalJobCount);

    bool IsRefreshScheduled(const NChunkClient::TChunkId& chunkId);

    DECLARE_ENUM(EScheduleFlags,
        ((None)(0x0000))
        ((Scheduled)(0x0001))
        ((Purged)(0x0002))
    );

    EScheduleFlags ScheduleReplicationJob(
        const THolder& sourceHolder,
        const NChunkClient::TChunkId& chunkId,
        yvector<NProto::TJobStartInfo>* jobsToStart);
    EScheduleFlags ScheduleBalancingJob(
        const THolder& sourceHolder,
        const NChunkClient::TChunkId& chunkId,
        yvector<NProto::TJobStartInfo>* jobsToStart);
    EScheduleFlags ScheduleRemovalJob(
        const THolder& holder,
        const NChunkClient::TChunkId& chunkId,
        yvector<NProto::TJobStartInfo>* jobsToStart);
    void ScheduleJobs(
        const THolder& holder,
        int maxReplicationJobsToStart,
        int maxRemovalJobsToStart,
        yvector<NProto::TJobStartInfo>* jobsToStart);

    void ScheduleRefresh(const NChunkClient::TChunkId& chunkId);
    void Refresh(const TChunk& chunk);
    int GetDesiredReplicaCount(const TChunk& chunk);
    void GetReplicaStatistics(
        const TChunk& chunk,
        int* desiredCount,
        int* realCount,
        int* plusCount,
        int* minusCount);
    void ScheduleNextRefresh();
    void OnRefresh();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
