#pragma once

#include "chunk_manager.h"
#include "chunk_placement.h"

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

    void RegisterHolder(const THolder& holder);
    void UnregisterHolder(const THolder& holder);

    void StartRefresh(IInvoker::TPtr invoker);
    void StopRefresh();
    void ScheduleRefresh(const TChunkId& chunkId);

    void RunJobControl(
        const THolder& holder,
        const yvector<NProto::TJobInfo>& runningJobs,
        yvector<NProto::TJobStartInfo>* jobsToStart,
        yvector<TJobId>* jobsToStop);

private:
    DECLARE_ENUM(EChunkState,
        (Lost)
        (OK)
        (Overreplicated)
        (Underreplicated)
    );

    TChunkManager::TPtr ChunkManager;
    TChunkPlacement::TPtr ChunkPlacement;

    struct TRefreshEntry
    {
        TChunkId ChunkId;
        TInstant When;
    };

    IInvoker::TPtr Invoker;
    yhash_set<TChunkId> RefreshSet;
    ydeque<TRefreshEntry> RefreshList;

    struct THolderInfo
        : public TRefCountedBase
    {
        typedef TIntrusivePtr<THolderInfo> TPtr;
        typedef yhash_set<TChunkId> TChunkIds;

        TChunkIds OverreplicatedChunks;
        TChunkIds UnderreplicatedChunks;
    };

    typedef yhash_map<THolderId, THolderInfo::TPtr> THolderInfoMap;
    THolderInfoMap HolderInfoMap;

    THolderInfo::TPtr FindHolderInfo(THolderId holderId);

    void ProcessRunningJobs(
        const THolder& holder,
        const yvector<NProto::TJobInfo>& runningJobs,
        yvector<TJobId>* jobsToStop,
        int* replicationJobCount,
        int* removalJobCount);

    bool IsJobScheduled(
        const yvector<NProto::TJobInfo>& runningJobs,
        const TChunkId& chunkId);

    bool IsRefreshScheduled(const TChunkId& chunkId);

    yvector<Stroka> GetTargetAddresses(const TChunk& chunk, int replicaCount);

    bool TryScheduleReplicationJob(
        const THolder& holder,
        const TChunkId& chunkId,
        const yvector<NProto::TJobInfo>& runningJobs,
        yvector<NProto::TJobStartInfo>* jobsToStart);
    bool TryScheduleRemovalJob(
        const THolder& holder,
        const TChunkId& chunkId,
        const yvector<NProto::TJobInfo>& runningJobs,
        yvector<NProto::TJobStartInfo>* jobsToStart);
    void ScheduleJobs(
        const THolder& holder,
        const yvector<NProto::TJobInfo>& runningJobs,
        int maxReplicationJobsToStart,
        int maxRemovalJobsToStart,
        yvector<NProto::TJobStartInfo>* jobsToStart);

    void Refresh(const TChunk& chunk);
    int GetDesiredReplicaCount(const TChunk& chunk);
    void GetReplicaStatistics(
        const TChunk& chunk,
        int* desiredCount,
        int* realCount,
        int* plusCount,
        int* minusCount,
        EChunkState* state);
    void ScheduleNextRefresh();
    void OnRefresh();

};


////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
