#include "stdafx.h"
#include "chunk_replication.h"

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/string.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkReplication::TChunkReplication(
    TChunkManager* chunkManager,
    TChunkPlacement* chunkPlacement,
    TChunkManager::TConfig* config,
    IInvoker* invoker)
    : ChunkManager(chunkManager)
    , ChunkPlacement(chunkPlacement)
    , Config(config)
    , Invoker(invoker)
{
    YASSERT(chunkManager);
    YASSERT(chunkPlacement);
    YASSERT(config);
    YASSERT(invoker);

    ScheduleNextRefresh();
}

void TChunkReplication::RunJobControl(
    const THolder& holder,
    const yvector<TJobInfo>& runningJobs,
    yvector<TJobStartInfo>* jobsToStart,
    yvector<TJobId>* jobsToStop)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    int replicationJobCount;
    int removalJobCount;
    ProcessExistingJobs(
        holder,
        runningJobs,
        jobsToStop,
        &replicationJobCount,
        &removalJobCount);

    ScheduleJobs(
        holder,
        Max(0, Config->MaxReplicationFanOut - replicationJobCount),
        Max(0, Config->MaxRemovalJobsPerHolder - removalJobCount),
        jobsToStart);
}

void TChunkReplication::OnHolderRegistered(const THolder& holder)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YVERIFY(HolderInfoMap.insert(MakePair(holder.GetId(), THolderInfo())).Second());

    FOREACH(const auto& chunk, holder.StoredChunkIds()) {
        ScheduleRefresh(chunk);
    }
}

void TChunkReplication::OnHolderUnregistered(const THolder& holder)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YVERIFY(HolderInfoMap.erase(holder.GetId()) == 1);
}

void TChunkReplication::OnReplicaAdded(const THolder& holder, const TChunk& chunk)
{
    UNUSED(holder);
    VERIFY_THREAD_AFFINITY(StateThread);

    ScheduleRefresh(chunk.GetId());
}

void TChunkReplication::OnReplicaRemoved(const THolder& holder, const TChunk& chunk)
{
    UNUSED(holder);
    VERIFY_THREAD_AFFINITY(StateThread);

    ScheduleRefresh(chunk.GetId());
}

void TChunkReplication::ScheduleChunkRemoval(const THolder& holder, const TChunkId& chunkId)
{
    auto& holderInfo = GetHolderInfo(holder.GetId());
    holderInfo.ChunksToReplicate.erase(chunkId);
    holderInfo.ChunksToRemove.insert(chunkId);
}

void TChunkReplication::ProcessExistingJobs(
    const THolder& holder,
    const yvector<TJobInfo>& runningJobs,
    yvector<TJobId>* jobsToStop,
    int* replicationJobCount,
    int* removalJobCount)
{
    *replicationJobCount = 0;
    *removalJobCount = 0;

    yhash_set<TJobId> runningJobIds;

    // TODO: check for timed out jobs
    FOREACH(const auto& jobInfo, runningJobs) {
        auto jobId = TJobId::FromProto(jobInfo.job_id());
        runningJobIds.insert(jobId);
        const auto* job = ChunkManager->FindJob(jobId);

        if (!job) {
            LOG_WARNING("Stopping unknown or obsolete job (JobId: %s, Address: %s, HolderId: %d)",
                ~jobId.ToString(),
                ~holder.GetAddress(),
                holder.GetId());
            jobsToStop->push_back(jobId);
            continue;
        }

        auto jobState = EJobState(jobInfo.state());
        switch (jobState) {
            case EJobState::Running:
                switch (job->GetType()) {
                    case EJobType::Replicate:
                        ++*replicationJobCount;
                        break;

                    case EJobType::Remove:
                        ++*removalJobCount;
                        break;

                    default:
                        YUNREACHABLE();
                }
                LOG_INFO("Job running (JobId: %s, HolderId: %d)",
                    ~jobId.ToString(),
                    holder.GetId());

                if (TInstant::Now() - job->GetStartTime() > Config->MaxJobDuration) {
                    jobsToStop->push_back(jobId);

                    LOG_INFO("Job duration limit exceeded (JobId: %s, HolderId: %d, Duration: %d ms, MaxDuration: %d ms)",
                        ~jobId.ToString(),
                        holder.GetId(),
                        static_cast<i32>((TInstant::Now() - job->GetStartTime()).MilliSeconds()),
                        static_cast<i32>(Config->MaxJobDuration.MilliSeconds()));
                }
                break;

            case EJobState::Completed:
                jobsToStop->push_back(jobId);
                ScheduleRefresh(job->GetChunkId());
                LOG_INFO("Job completed (JobId: %s, HolderId: %d)",
                    ~jobId.ToString(),
                    holder.GetId());
                break;

            case EJobState::Failed:
                jobsToStop->push_back(jobId);
                ScheduleRefresh(job->GetChunkId());
                LOG_WARNING("Job failed (JobId: %s, HolderId: %d)",
                    ~jobId.ToString(),
                    holder.GetId());
                break;

            default:
                YUNREACHABLE();
        }
    }

    // Checking for missing jobs
    FOREACH(auto jobId, holder.JobIds()) {
        if (runningJobIds.find(jobId) == runningJobIds.end()) {
            LOG_WARNING("Job is missing (JobId: %s, Address: %s, HolderId: %d)",
                ~jobId.ToString(),
                ~holder.GetAddress(),
                holder.GetId());
            jobsToStop->push_back(jobId);
        }
    }
}

bool TChunkReplication::IsRefreshScheduled(const TChunkId& chunkId)
{
    return RefreshSet.find(chunkId) != RefreshSet.end();
}

TChunkReplication::EScheduleFlags TChunkReplication::ScheduleReplicationJob(
    const THolder& sourceHolder,
    const TChunkId& chunkId,
    yvector<TJobStartInfo>* jobsToStart)
{
    const auto* chunk = ChunkManager->FindChunk(chunkId);
    if (!chunk) {
        LOG_INFO("Chunk we're about to replicate is missing (ChunkId: %s, Address: %s, HolderId: %d)",
            ~chunkId.ToString(),
            ~sourceHolder.GetAddress(),
            sourceHolder.GetId());
        return EScheduleFlags::Purged;
    }

    if (IsRefreshScheduled(chunkId)) {
        LOG_INFO("Chunk we're about to replicate is scheduled for another refresh (ChunkId: %s, Address: %s, HolderId: %d)",
            ~chunkId.ToString(),
            ~sourceHolder.GetAddress(),
            sourceHolder.GetId());
        return EScheduleFlags::None;
    }

    int desiredCount;
    int storedCount;
    int cachedCount;
    int plusCount;
    int minusCount;
    GetReplicaStatistics(
        *chunk,
        &desiredCount,
        &storedCount,
        &cachedCount,
        &plusCount,
        &minusCount);

    int requestedCount = desiredCount - (storedCount + plusCount);
    if (requestedCount <= 0) {
        // TODO: is this possible?
        LOG_INFO("Chunk we're about to replicate has enough replicas (ChunkId: %s, Address: %s, HolderId: %d)",
            ~chunkId.ToString(),
            ~sourceHolder.GetAddress(),
            sourceHolder.GetId());
        return EScheduleFlags::Purged;
    }

    auto targets = ChunkPlacement->GetReplicationTargets(*chunk, requestedCount);
    if (targets.empty()) {
        LOG_DEBUG("No suitable target holders for replication (ChunkId: %s, HolderId: %d)",
            ~chunkId.ToString(),
            sourceHolder.GetId());
        return EScheduleFlags::None;
    }

    yvector<Stroka> targetAddresses;
    FOREACH (auto holderId, targets) {
        const auto& holder = ChunkManager->GetHolder(holderId);
        targetAddresses.push_back(holder.GetAddress());
        ChunkPlacement->OnSessionHinted(holder);
    }

    auto jobId = TJobId::Create();
    TJobStartInfo startInfo;
    startInfo.set_job_id(jobId.ToProto());
    startInfo.set_type(EJobType::Replicate);
    startInfo.set_chunk_id(chunkId.ToProto());
    ToProto(*startInfo.mutable_target_addresses(), targetAddresses);
    jobsToStart->push_back(startInfo);

    LOG_INFO("Chunk replication scheduled (ChunkId: %s, Address: %s, HolderId: %d, JobId: %s, TargetAddresses: [%s])",
        ~chunkId.ToString(),
        ~sourceHolder.GetAddress(),
        sourceHolder.GetId(),
        ~jobId.ToString(),
        ~JoinToString(targetAddresses));

    return
        targetAddresses.ysize() == requestedCount
        // TODO: flagged enums
        ? (EScheduleFlags) (EScheduleFlags::Purged | EScheduleFlags::Scheduled)
        : (EScheduleFlags) EScheduleFlags::Scheduled;
}

TChunkReplication::EScheduleFlags TChunkReplication::ScheduleBalancingJob(
    const THolder& sourceHolder,
    const TChunkId& chunkId,
    yvector<TJobStartInfo>* jobsToStart)
{
    const auto& chunk = ChunkManager->GetChunk(chunkId);

    if (IsRefreshScheduled(chunkId)) {
        LOG_DEBUG("Postponed chunk balancing until another refresh (ChunkId: %s, Address: %s, HolderId: %d)",
            ~chunkId.ToString(),
            ~sourceHolder.GetAddress(),
            sourceHolder.GetId());
        return EScheduleFlags::None;
    }

    double maxFillCoeff =
        ChunkPlacement->GetFillCoeff(sourceHolder) -
        Config->MinChunkBalancingFillCoeffDiff;
    auto targetHolderId = ChunkPlacement->GetBalancingTarget(chunk, maxFillCoeff);
    if (targetHolderId == InvalidHolderId) {
        LOG_DEBUG("No suitable target holders for balancing (ChunkId: %s, Address: %s, HolderId: %d)",
            ~chunkId.ToString(),
            ~sourceHolder.GetAddress(),
            sourceHolder.GetId());
        return EScheduleFlags::None;
    }

    const auto& targetHolder = ChunkManager->GetHolder(targetHolderId);
    ChunkPlacement->OnSessionHinted(targetHolder);
    
    auto jobId = TJobId::Create();
    TJobStartInfo startInfo;
    startInfo.set_job_id(jobId.ToProto());
    startInfo.set_type(EJobType::Replicate);
    startInfo.set_chunk_id(chunkId.ToProto());
    startInfo.add_target_addresses(targetHolder.GetAddress());
    jobsToStart->push_back(startInfo);

    LOG_INFO("Chunk balancing scheduled (ChunkId: %s, Address: %s, HolderId: %d, JobId: %s, TargetAddress: %s)",
        ~chunkId.ToString(),
        ~sourceHolder.GetAddress(),
        sourceHolder.GetId(),
        ~jobId.ToString(),
        ~targetHolder.GetAddress());

    // TODO: flagged enums
    return (EScheduleFlags) (EScheduleFlags::Purged | EScheduleFlags::Scheduled);
}

TChunkReplication::EScheduleFlags TChunkReplication::ScheduleRemovalJob(
    const THolder& holder,
    const TChunkId& chunkId,
    yvector<TJobStartInfo>* jobsToStart)
{
    if (IsRefreshScheduled(chunkId)) {
        LOG_DEBUG("Postponed chunk removal until another refresh (ChunkId: %s, Address: %s, HolderId: %d)",
            ~chunkId.ToString(),
            ~holder.GetAddress(),
            holder.GetId());
        return EScheduleFlags::None;
    }
    
    LostChunkIds_.erase(chunkId);
    UnderreplicatedChunkIds_.erase(chunkId);
    OverreplicatedChunkIds_.erase(chunkId);

    auto jobId = TJobId::Create();
    TJobStartInfo startInfo;
    startInfo.set_job_id(jobId.ToProto());
    startInfo.set_type(EJobType::Remove);
    startInfo.set_chunk_id(chunkId.ToProto());
    jobsToStart->push_back(startInfo);

    LOG_INFO("Removal job scheduled (ChunkId: %s, Address: %s, HolderId: %d, JobId: %s)",
        ~chunkId.ToString(),
        ~holder.GetAddress(),
        holder.GetId(),
        ~jobId.ToString());

    // TODO: flagged enums
    return (EScheduleFlags) (EScheduleFlags::Purged | EScheduleFlags::Scheduled);
}

void TChunkReplication::ScheduleJobs(
    const THolder& holder,
    int maxReplicationJobsToStart,
    int maxRemovalJobsToStart,
    yvector<TJobStartInfo>* jobsToStart)
{
    auto* holderInfo = FindHolderInfo(holder.GetId());
    if (!holderInfo)
        return;

    // Schedule replication jobs.
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

    // Schedule balancing jobs.
    if (maxReplicationJobsToStart > 0 &&
        ChunkPlacement->GetFillCoeff(holder) > Config->MinChunkBalancingFillCoeff)
    {
        auto chunksToBalance = ChunkPlacement->GetBalancingChunks(holder, maxReplicationJobsToStart);
        if (!chunksToBalance.empty()) {
            LOG_DEBUG("Holder is eligible for balancing (Address: %s, HolderId: %d, ChunkIds: [%s])",
                ~holder.GetAddress(),
                holder.GetId(),
                ~JoinToString(chunksToBalance));

            FOREACH (const auto& chunkId, chunksToBalance) {
                auto flags = ScheduleBalancingJob(holder, chunkId, jobsToStart);
                if (flags & EScheduleFlags::Scheduled) {
                    --maxReplicationJobsToStart;
                }
            }
        }
    }

    // Schedule removal jobs.
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
    int* storedCount,
    int* cachedCount,
    int* plusCount,
    int* minusCount)
{
    *desiredCount = GetDesiredReplicaCount(chunk);
    *storedCount = chunk.StoredLocations().ysize();
    *cachedCount = !~chunk.CachedLocations() ? 0 : static_cast<int>(chunk.CachedLocations()->size());
    *plusCount = 0;
    *minusCount = 0;

    if (*storedCount == 0) {
        return;
    }

    const auto* jobList = ChunkManager->FindJobList(chunk.GetId());
    if (jobList) {
        yhash_set<Stroka> storedAddresses(*storedCount);
        FOREACH(auto holderId, chunk.StoredLocations()) {
            const auto& holder = ChunkManager->GetHolder(holderId);
            storedAddresses.insert(holder.GetAddress());
        }

        FOREACH(const auto& jobId, jobList->JobIds()) {
            const auto& job = ChunkManager->GetJob(jobId);
            switch (job.GetType()) {
                case EJobType::Replicate: {
                    FOREACH(const auto& address, job.TargetAddresses()) {
                        if (storedAddresses.find(address) == storedAddresses.end()) {
                            ++*plusCount;
                        }
                    }
                    break;
                }

                case EJobType::Remove:
                    if (storedAddresses.find(job.GetRunnerAddress()) != storedAddresses.end()) {
                        ++*minusCount;
                    }
                    break;

                default:
                    YUNREACHABLE();
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
    int storedCount;
    int cachedCount;
    int plusCount;
    int minusCount;
    GetReplicaStatistics(
        chunk,
        &desiredCount,
        &storedCount,
        &cachedCount,
        &plusCount,
        &minusCount);

    auto replicaCountStr = Sprintf("%d+%d+%d-%d",
        storedCount,
        cachedCount,
        plusCount,
        minusCount);

    FOREACH(auto holderId, chunk.StoredLocations()) {
        auto* holderInfo = FindHolderInfo(holderId);
        if (holderInfo) {
            holderInfo->ChunksToReplicate.erase(chunk.GetId());
            holderInfo->ChunksToRemove.erase(chunk.GetId());
        }
    }
    auto chunkId = chunk.GetId();
    LostChunkIds_.erase(chunkId);
    OverreplicatedChunkIds_.erase(chunkId);
    UnderreplicatedChunkIds_.erase(chunkId);

    if (storedCount == 0) {
        LOG_INFO("Chunk is lost (ChunkId: %s, ReplicaCount: %s, DesiredReplicaCount: %d)",
            ~chunk.GetId().ToString(),
            ~replicaCountStr,
            desiredCount);
        LostChunkIds_.insert(chunkId);
    } else if (storedCount - minusCount > desiredCount) {
        OverreplicatedChunkIds_.insert(chunkId);

        // NB: Never start removal jobs if new replicas are on the way, hence the check plusCount > 0.
        if (plusCount > 0) {
            LOG_WARNING("Chunk is over-replicated, waiting for pending replications to complete (ChunkId: %s, ReplicaCount: %s, DesiredReplicaCount: %d)",
                ~chunk.GetId().ToString(),
                ~replicaCountStr,
                desiredCount);
            return;
        }

        auto holderIds = ChunkPlacement->GetRemovalTargets(chunk, storedCount - minusCount - desiredCount);
        FOREACH(auto holderId, holderIds) {
            auto& holderInfo = GetHolderInfo(holderId);
            holderInfo.ChunksToRemove.insert(chunk.GetId());
        }

        yvector<Stroka> holderAddresses;
        FOREACH(auto holderId, holderIds) {
            const auto& holder = ChunkManager->GetHolder(holderId);
            holderAddresses.push_back(holder.GetAddress());
        }

        LOG_INFO("Chunk is over-replicated, removal is scheduled at [%s] (ChunkId: %s, ReplicaCount: %s, DesiredReplicaCount: %d)",
            ~JoinToString(holderAddresses),
            ~chunk.GetId().ToString(),
            ~replicaCountStr,
            desiredCount);
    } else if (storedCount + plusCount < desiredCount) {
        UnderreplicatedChunkIds_.insert(chunkId);

        // NB: Never start replication jobs when removal jobs are in progress, hence the check minusCount > 0.
        if (minusCount > 0) {
            LOG_WARNING("Chunk is under-replicated, waiting for pending removals to complete (ChunkId: %s, ReplicaCount: %s, DesiredReplicaCount: %d)",
                ~chunk.GetId().ToString(),
                ~replicaCountStr,
                desiredCount);
            return;
        }

        auto holderId = ChunkPlacement->GetReplicationSource(chunk);
        auto& holderInfo = GetHolderInfo(holderId);
        const auto& holder = ChunkManager->GetHolder(holderId);

        holderInfo.ChunksToReplicate.insert(chunk.GetId());

        LOG_INFO("Chunk is under-replicated, replication is scheduled at %s (ChunkId: %s, ReplicaCount: %s, DesiredReplicaCount: %d)",
            ~holder.GetAddress(),
            ~chunk.GetId().ToString(),
            ~replicaCountStr,
            desiredCount);
    } else {
        LOG_INFO("Chunk is OK (ChunkId: %s, ReplicaCount: %s, DesiredReplicaCount: %d)",
            ~chunk.GetId().ToString(),
            ~replicaCountStr,
            desiredCount);
    }
 }

void TChunkReplication::ScheduleRefresh(const TChunkId& chunkId)
{
    if (RefreshSet.find(chunkId) != RefreshSet.end())
        return;

    TRefreshEntry entry;
    entry.ChunkId = chunkId;
    entry.When = TInstant::Now() + Config->ChunkRefreshDelay;
    RefreshList.push_back(entry);
    RefreshSet.insert(chunkId);
}

void TChunkReplication::ScheduleNextRefresh()
{
    TDelayedInvoker::Submit(
        ~FromMethod(&TChunkReplication::OnRefresh, TPtr(this))
        ->Via(Invoker),
        Config->ChunkRefreshQuantum);
}

void TChunkReplication::OnRefresh()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto now = TInstant::Now();
    for (int i = 0; i < Config->MaxChunksPerRefresh; ++i) {
        if (RefreshList.empty())
            break;

        const auto& entry = RefreshList.front();
        if (entry.When > now)
            break;

        auto* chunk = ChunkManager->FindChunk(entry.ChunkId);
        if (chunk) {
            Refresh(*chunk);
        }

        YVERIFY(RefreshSet.erase(entry.ChunkId) == 1);
        RefreshList.pop_front();
    }
    ScheduleNextRefresh();
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
