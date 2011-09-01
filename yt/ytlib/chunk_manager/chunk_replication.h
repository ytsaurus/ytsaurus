#pragma once

#include "chunk_manager.h"
#include "chunk_placement.h"

#include <util/generic/deque.h>

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EChunkState,
    (Lost)
    (OK)
    (Overreplicated)
    (Underreplicated)
);

class TChunkReplication
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunkReplication> TPtr;

    // TODO: to cpp
    TChunkReplication(
        TChunkManager::TPtr chunkManager,
        TChunkPlacement::TPtr chunkPlacement)
        : ChunkManager(chunkManager)
        , ChunkPlacement(chunkPlacement)
    { }

    void StartRefresh(IInvoker::TPtr invoker);
    void StopRefresh();

    void ScheduleRefresh(TChunk& chunk);

    void RunJobControl(
        const THolder& holder,
        const yvector<NProto::TJobInfo>& runningJobs,
        yvector<NProto::TJobStartInfo>* jobsToStart,
        yvector<TJobId>* jobsToStop);

private:
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

    void ProcessRunningJobs(
        const THolder& holder,
        const yvector<NProto::TJobInfo>& runningJobs,
        yvector<TJobId>* jobsToStop,
        int* replicationJobCount,
        int* removalJobCount);

    bool IsJobScheduled(
        const yvector<NProto::TJobInfo>& runningJobs,
        const TChunkId& chunkId);

    yvector<Stroka> GetTargetAddresses(const TChunk& chunk, int replicaCount);

    void ScheduleReplicationJobs(
        const THolder& holder,
        const yvector<NProto::TJobInfo>& runningJobs,
        int maxJobsToStart,
        yvector<NProto::TJobStartInfo>* jobsToStart);

    void ScheduleRemovalJobs(
        const THolder& holder,
        const yvector<NProto::TJobInfo>& runningJobs,
        int maxJobsToStart,
        yvector<NProto::TJobStartInfo>* jobsToStart);

    void Refresh(TChunk& chunk);

    EChunkState GetState(const TChunk& chunk);

    int GetDesiredReplicaCount(const TChunk& chunk);

    void CountReplicas(
        const TChunk& chunk,
        int* real,
        int* plusDelta,
        int* minusDelta);

    bool UpdateState(TChunk& chunk, EChunkState state);

    void ScheduleNextRefresh();
    void OnRefresh();

};


////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
