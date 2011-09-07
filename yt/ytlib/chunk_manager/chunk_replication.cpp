#include "chunk_replication.h"

#include "../misc/serialize.h"
#include "../misc/string.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

// TODO: make configurable
static int MaxReplicationJobsPerHolder = 4;
static int MaxRemovalJobsPerHolder = 16;
static TDuration ChunkRefreshDelay = TDuration::Seconds(15);
static TDuration ChunkRefreshQuantum = TDuration::MilliSeconds(100);
static int MaxChunksPerRefresh = 1000;

static NLog::TLogger& Logger = ChunkManagerLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkReplication::TChunkReplication(
    TChunkManager::TPtr chunkManager,
    TChunkPlacement::TPtr chunkPlacement)
    : ChunkManager(chunkManager)
    , ChunkPlacement(chunkPlacement)
{ }

void TChunkReplication::RunJobControl(
    const THolder& holder,
    const yvector<NProto::TJobInfo>& runningJobs,
    yvector<NProto::TJobStartInfo>* jobsToStart,
    yvector<TJobId>* jobsToStop )
{
    int replicationJobCount;
    int removalJobCount;
    ProcessRunningJobs(
        holder,
        runningJobs,
        jobsToStop,
        &replicationJobCount,
        &removalJobCount);

    ScheduleJobs(
        holder,
        Max(0, MaxReplicationJobsPerHolder - replicationJobCount),
        Max(0, MaxRemovalJobsPerHolder - removalJobCount),
        jobsToStart);
}

void TChunkReplication::RegisterHolder(const THolder& holder)
{
    YVERIFY(HolderInfoMap.insert(MakePair(holder.Id, THolderInfo())).Second());

    for (auto it = holder.Chunks.begin();
         it != holder.Chunks.end();
         ++it)
    {
        ScheduleRefresh(*it);
    }
}

void TChunkReplication::UnregisterHolder(const THolder& holder)
{
    YVERIFY(HolderInfoMap.erase(holder.Id) == 1);
}

void TChunkReplication::RegisterReplica(const THolder& holder, const TChunk& chunk)
{
    UNUSED(holder);
    ScheduleRefresh(chunk.Id);
}

void TChunkReplication::UnregisterReplica(const THolder& holder, const TChunk& chunk)
{
    UNUSED(holder);
    ScheduleRefresh(chunk.Id);
}

void TChunkReplication::ProcessRunningJobs(
    const THolder& holder,
    const yvector<NProto::TJobInfo>& runningJobs,
    yvector<TJobId>* jobsToStop,
    int* replicationJobCount,
    int* removalJobCount)
{
    *replicationJobCount = 0;
    *removalJobCount = 0;

    // TODO: check for missing jobs
    for (auto it = runningJobs.begin();
        it != runningJobs.end();
        ++it)
    {
        auto jobInfo = *it;
        auto jobId = TJobId::FromProto(jobInfo.GetJobId());
        const auto& job = ChunkManager->GetJob(jobId);
        auto jobState = EJobState(jobInfo.GetState());
        switch (jobState) {
        case EJobState::Running:
            switch (job.Type) {
                case EJobType::Replicate:
                    ++*replicationJobCount;
                    break;

                case EJobType::Remove:
                    ++*removalJobCount;
                    break;

                default:
                    YASSERT(false);
                    break;
            }
            LOG_INFO("Job running (JobId: %s, HolderId: %d)",
                ~jobId.ToString(),
                holder.Id);
            break;

        case EJobState::Completed:
            jobsToStop->push_back(jobId);
            LOG_INFO("Job completed (JobId: %s, HolderId: %d)",
                ~jobId.ToString(),
                holder.Id);
            break;

        case EJobState::Failed:
            jobsToStop->push_back(jobId);
            LOG_WARNING("Job failed (JobId: %s, HolderId: %d)",
                ~jobId.ToString(),
                holder.Id);
            break;

        default:
            YASSERT(false);
            break;
        }
    }
}

bool TChunkReplication::IsRefreshScheduled(const TChunkId& chunkId)
{
    return RefreshSet.find(chunkId) != RefreshSet.end();
}

yvector<Stroka> TChunkReplication::GetTargetAddresses(
    const TChunk& chunk,
    int replicaCount)
{
    yhash_set<Stroka> forbiddenAddresses;

    const TChunk::TLocations& locations = chunk.Locations;
    for (auto it = locations.begin();
        it != locations.end();
        ++it)
    {
        const auto& holder = ChunkManager->GetHolder(*it);
        forbiddenAddresses.insert(holder.Address);
    }

    const auto* jobList = ChunkManager->FindJobList(chunk.Id);
    if (jobList != NULL) {
        const auto& jobs = jobList->Jobs;
        for (auto it = jobs.begin();
            it != jobs.end();
            ++it)
        {
            const auto& job = ChunkManager->GetJob(*it);
            if (job.Type == EJobType::Replicate && job.ChunkId == chunk.Id) {
                forbiddenAddresses.insert(job.TargetAddresses.begin(), job.TargetAddresses.end());
            }
        }
    }

    auto candidateHolders = ChunkPlacement->GetTargetHolders(replicaCount + forbiddenAddresses.size());

    yvector<Stroka> targetAddresses;
    for (auto it = candidateHolders.begin();
        it != candidateHolders.end() && targetAddresses.ysize() < replicaCount;
        ++it)
    {
        const auto& holder = ChunkManager->GetHolder(*it);
        if (forbiddenAddresses.find(holder.Address) == forbiddenAddresses.end()) {
            targetAddresses.push_back(holder.Address);
        }
    }

    return targetAddresses;
}

TChunkReplication::EScheduleFlags TChunkReplication::ScheduleReplicationJob(
    const THolder& holder,
    const TChunkId& chunkId,
    yvector<NProto::TJobStartInfo>* jobsToStart)
{
    const auto* chunk = ChunkManager->FindChunk(chunkId);
    if (chunk == NULL) {
        LOG_INFO("Chunk for replication is missing (ChunkId: %s, HolderId: %d)",
            ~chunkId.ToString(),
            holder.Id);
        return EScheduleFlags::Purged;
    }

    if (IsRefreshScheduled(chunkId)) {
        LOG_INFO("Chunk for replication is scheduled for another refresh (ChunkId: %s, HolderId: %d)",
            ~chunkId.ToString(),
            holder.Id);
        return EScheduleFlags::None;
    }

    int desiredCount;
    int realCount;
    int plusCount;
    int minusCount;
    GetReplicaStatistics(
        *chunk,
        &desiredCount,
        &realCount,
        &plusCount,
        &minusCount);

    int requestedCount = desiredCount - (realCount + plusCount);
    if (requestedCount <= 0) {
        // TODO: is this possible?
        LOG_INFO("Chunk for replication has enough replicas (ChunkId: %s, HolderId: %d)",
            ~chunkId.ToString(),
            holder.Id);
        return EScheduleFlags::Purged;
    }

    auto targetAddresses = GetTargetAddresses(*chunk, requestedCount);
    if (targetAddresses.empty()) {
        LOG_INFO("No suitable target holders for replication (ChunkId: %s, HolderId: %d)",
            ~chunkId.ToString(),
            holder.Id);
        return EScheduleFlags::None;
    }

    auto jobId = TJobId::Create();
    NProto::TJobStartInfo startInfo;
    startInfo.SetJobId(jobId.ToProto());
    startInfo.SetType(EJobType::Replicate);
    startInfo.SetChunkId(chunkId.ToProto());
    ToProto(*startInfo.MutableTargetAddresses(), targetAddresses);
    jobsToStart->push_back(startInfo);

    LOG_INFO("Chunk replication scheduled (ChunkId: %s, HolderId: %d, JobId: %s, TargetAddresses: [%s])",
        ~chunkId.ToString(),
        holder.Id,
        ~jobId.ToString(),
        ~JoinToString(targetAddresses, ", "));

    return
        (targetAddresses.ysize() == requestedCount)
        // TODO: flagged enums
        ? (EScheduleFlags) (EScheduleFlags::Purged | EScheduleFlags::Scheduled)
        : EScheduleFlags::Scheduled;
}

TChunkReplication::EScheduleFlags TChunkReplication::ScheduleRemovalJob(
    const THolder& holder,
    const TChunkId& chunkId,
    yvector<NProto::TJobStartInfo>* jobsToStart )
{
    const auto* chunk = ChunkManager->FindChunk(chunkId);
    if (chunk == NULL) {
        LOG_INFO("Chunk for removal is missing (ChunkId: %s, HolderId: %d)",
            ~chunkId.ToString(),
            holder.Id);
        return EScheduleFlags::Purged;
    }

    if (IsRefreshScheduled(chunkId)) {
        LOG_INFO("Chunk for removal is scheduled for another refresh (ChunkId: %s, HolderId: %d)",
            ~chunkId.ToString(),
            holder.Id);
        return EScheduleFlags::None;
    }
    
    auto jobId = TJobId::Create();
    NProto::TJobStartInfo startInfo;
    startInfo.SetJobId(jobId.ToProto());
    startInfo.SetType(EJobType::Remove);
    startInfo.SetChunkId(chunkId.ToProto());
    jobsToStart->push_back(startInfo);

    LOG_INFO("Removal job scheduled (ChunkId: %s, HolderId: %d, JobId: %s)",
        ~chunkId.ToString(),
        holder.Id,
        ~jobId.ToString());

    // TODO: flagged enums
    return (EScheduleFlags) (EScheduleFlags::Purged | EScheduleFlags::Scheduled);
}

void TChunkReplication::ScheduleJobs(
    const THolder& holder,
    int maxReplicationJobsToStart,
    int maxRemovalJobsToStart,
    yvector<NProto::TJobStartInfo>* jobsToStart)
{
    auto* holderInfo = FindHolderInfo(holder.Id);
    if (holderInfo == NULL)
        return;

    {
        auto& chunksToReplicate = holderInfo->ChunksToReplicate;
        auto it = chunksToReplicate.begin();
        while (it != chunksToReplicate.end() && maxReplicationJobsToStart > 0) {
            auto jt = it;
            ++jt;
            const auto& chunkId = *it;
            auto flags = ScheduleReplicationJob(holder, chunkId, jobsToStart);
            if (flags & EScheduleFlags::Scheduled) {
                --maxReplicationJobsToStart;
            }
            if (flags & EScheduleFlags::Purged) {
                chunksToReplicate.erase(it);
            }
            it = jt;
        }
    }

    {
        auto& chunksToRemove = holderInfo->ChunksToRemove;
        auto it = chunksToRemove.begin();
        while (it != chunksToRemove.end() && maxRemovalJobsToStart > 0) {
            const auto& chunkId = *it;
            auto jt = it;
            ++jt;
            auto flags = ScheduleRemovalJob(holder, chunkId, jobsToStart);
            if (flags & EScheduleFlags::Scheduled) {
                --maxReplicationJobsToStart;
            }
            if (flags & EScheduleFlags::Purged) {
                chunksToRemove.erase(it);
            }
            it = jt;
        }
    }
}

void TChunkReplication::GetReplicaStatistics(
    const TChunk& chunk,
    int* desiredCount,
    int* realCount,
    int* plusCount,
    int* minusCount)
{
    const TChunk::TLocations& locations = chunk.Locations;

    *desiredCount = GetDesiredReplicaCount(chunk);
    *realCount = locations.ysize();
    *plusCount = 0;
    *minusCount = 0;

    if (*realCount == 0) {
        return;
    }

    const auto* jobList = ChunkManager->FindJobList(chunk.Id);
    if (jobList != NULL) {
        yhash_set<Stroka> realAddresses(*realCount);
        for (auto locationIt = locations.begin();
            locationIt != locations.end();
            ++locationIt)
        {
            const auto& holder = ChunkManager->GetHolder(*locationIt);
            realAddresses.insert(holder.Address);
        }

        for (auto jobIt = jobList->Jobs.begin();
            jobIt != jobList->Jobs.end();
            ++jobIt)
        {
            const auto& job = ChunkManager->GetJob(*jobIt);
            switch (job.Type) {
            case EJobType::Replicate:
                for (auto targetIt = job.TargetAddresses.begin();
                    targetIt != job.TargetAddresses.end();
                    ++targetIt)
                {
                    if (realAddresses.find(*targetIt) == realAddresses.end()) {
                        ++*plusCount;
                    }
                }
                break;

            case EJobType::Remove:
                if (realAddresses.find(job.RunnerAddress) != realAddresses.end()) {
                    ++*minusCount;
                }
                break;

            default:
                YASSERT(false);
                break;
            }
        }
    }
}

int TChunkReplication::GetDesiredReplicaCount(const TChunk& chunk)
{
    // TODO: make configurable
    UNUSED(chunk);
    return 3;
}

void TChunkReplication::Refresh(const TChunk& chunk)
{
    int desiredCount;
    int realCount;
    int plusCount;
    int minusCount;
    GetReplicaStatistics(
        chunk,
        &desiredCount,
        &realCount,
        &plusCount,
        &minusCount);

    const auto& locations = chunk.Locations;
    for (auto it = locations.begin();
        it != locations.end();
        ++it)
    {
        auto* holderInfo = FindHolderInfo(*it);
        if (holderInfo != NULL) {
            holderInfo->ChunksToReplicate.erase(chunk.Id);
            holderInfo->ChunksToRemove.erase(chunk.Id);
        }
    }

    if (realCount == 0) {
        LOG_INFO("Chunk is lost (ChunkId: %s, ReplicaCount: %d+%d-%d, DesiredReplicaCount: %d)",
            ~chunk.Id.ToString(),
            realCount,
            plusCount,
            minusCount,
            desiredCount);
    } else if (realCount - minusCount > desiredCount) {
        // NB: never start removal jobs is new replicas are on the way, hence the check plusCount > 0.
        if (plusCount > 0) {
            LOG_INFO("Chunk is over-replicated, waiting for pending replications to complete (ChunkId: %s, ReplicaCount: %d+%d-%d, DesiredReplicaCount: %d)",
                ~chunk.Id.ToString(),
                realCount,
                plusCount,
                minusCount,
                desiredCount);
            return;
        }

        auto holderIds = GetHoldersForRemoval(chunk, realCount - minusCount - desiredCount);
        for (auto it = holderIds.begin(); it != holderIds.end(); ++it) {
            auto& holderInfo = GetHolderInfo(*it);
            holderInfo.ChunksToRemove.insert(chunk.Id);
        }

        yvector<Stroka> holderAddresses;
        for (auto it = holderIds.begin(); it != holderIds.end(); ++it) {
            const auto& holder = ChunkManager->GetHolder(*it);
            holderAddresses.push_back(holder.Address);
        }

        LOG_INFO("Chunk is over-replicated, removal is scheduled at [%s] (ChunkId: %s, ReplicaCount: %d+%d-%d, DesiredReplicaCount: %d)",
            ~JoinToString(holderAddresses, ", "),
            ~chunk.Id.ToString(),
            realCount,
            plusCount,
            minusCount,
            desiredCount);
    } else if (realCount + plusCount < desiredCount && minusCount == 0) {
        // NB: never start replication jobs when removal jobs are in progress, hence the check minusCount > 0.
        if (minusCount > 0) {
            LOG_INFO("Chunk is under-replicated, waiting for pending removals to complete (ChunkId: %s, ReplicaCount: %d+%d-%d, DesiredReplicaCount: %d)",
                ~chunk.Id.ToString(),
                realCount,
                plusCount,
                minusCount,
                desiredCount);
            return;
        }

        auto holderId = GetHolderForReplication(chunk);
        auto& holderInfo = GetHolderInfo(holderId);
        const auto& holder = ChunkManager->GetHolder(holderId);

        holderInfo.ChunksToReplicate.insert(chunk.Id);

        LOG_INFO("Chunk is under-replicated, replication is scheduled at %s (ChunkId: %s, ReplicaCount: %d+%d-%d, DesiredReplicaCount: %d)",
            ~holder.Address,
            ~chunk.Id.ToString(),
            realCount,
            plusCount,
            minusCount,
            desiredCount);
    } else {
        LOG_INFO("Chunk is OK (ChunkId: %s, ReplicaCount: %d+%d-%d, DesiredReplicaCount: %d)",
            ~chunk.Id.ToString(),
            realCount,
            plusCount,
            minusCount,
            desiredCount);
    }
 }

void TChunkReplication::ScheduleRefresh(const TChunkId& chunkId)
{
    if (RefreshSet.find(chunkId) != RefreshSet.end())
        return;

    TRefreshEntry entry;
    entry.ChunkId = chunkId;
    entry.When = TInstant::Now() + ChunkRefreshDelay;
    RefreshList.push_back(entry);
    RefreshSet.insert(chunkId);
}

void TChunkReplication::ScheduleNextRefresh()
{
    TDelayedInvoker::Get()->Submit(
        FromMethod(
            &TChunkReplication::OnRefresh,
            TPtr(this))
        ->Via(Invoker),
        ChunkRefreshQuantum);
}

void TChunkReplication::OnRefresh()
{
    auto now = TInstant::Now();
    for (int i = 0; i < MaxChunksPerRefresh; ++i) {
        if (RefreshList.empty())
            break;

        const auto& entry = RefreshList.front();
        if (entry.When > now)
            break;

        auto* chunk = ChunkManager->FindChunk(entry.ChunkId);
        if (chunk != NULL) {
            Refresh(*chunk);
        }

        YVERIFY(RefreshSet.erase(entry.ChunkId) == 1);
        RefreshList.pop_front();
    }
    ScheduleNextRefresh();
}

void TChunkReplication::StartRefresh( IInvoker::TPtr invoker )
{
    YASSERT(~Invoker == NULL);
    Invoker = invoker;
    ScheduleNextRefresh();
}

void TChunkReplication::StopRefresh()
{
    YASSERT(~Invoker != NULL);
    Invoker.Drop();
}

TChunkReplication::THolderInfo* TChunkReplication::FindHolderInfo(THolderId holderId)
{
    auto it = HolderInfoMap.find(holderId);
    return it == HolderInfoMap.end() ? NULL : &it->Second();
}

TChunkReplication::THolderInfo& TChunkReplication::GetHolderInfo(THolderId holderId)
{
    auto it = HolderInfoMap.find(holderId);
    YASSERT(it != HolderInfoMap.end());
    return it->Second();
}

NYT::NChunkManager::THolderId TChunkReplication::GetHolderForReplication(const TChunk& chunk)
{
    // TODO: pick the least loaded holder
    YASSERT(chunk.Locations.ysize() > 0);
    return chunk.Locations[0];
}

yvector<THolderId> TChunkReplication::GetHoldersForRemoval(const TChunk& chunk, int count)
{
    // TODO: pick the most loaded holder
    yvector<THolderId> result;
    result.reserve(count);
    const TChunk::TLocations& locations = chunk.Locations;
    for (auto it = locations.begin();
        it != chunk.Locations.end() && result.ysize() < count;
        ++it)
    {
        result.push_back(*it);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
