#pragma once

#include "common.h"
#include "chunk_manager.h"
#include "chunk_placement.h"

#include "../misc/thread_affinity.h"

#include <util/generic/deque.h>

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

class TChunkReplication
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunkReplication> TPtr;

    TChunkReplication(
        TChunkManager::TPtr chunkManager,
        TChunkPlacement::TPtr chunkPlacement);

    void AddHolder(const THolder& holder);
    void RemoveHolder(const THolder& holder);

    void AddReplica(const THolder& holder, const TChunk& chunk);
    void RemoveReplica(const THolder& holder, const TChunk& chunk);

    void Start(IInvoker::TPtr invoker);
    void Stop();

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
        const yvector<NProto::TJobInfo>& runningJobs,
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

    void ScheduleRefresh(const TChunkId& chunkId);
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

} // namespace NChunkManager
} // namespace NYT
