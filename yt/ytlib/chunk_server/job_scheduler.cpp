#include "stdafx.h"
#include "job_scheduler.h"
#include "holder_lease_tracker.h"
#include "chunk_placement.h"
#include "holder.h"
#include "job.h"
#include "chunk.h"
#include "job_list.h"

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/string.h>
#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/cell_master/config.h>
#include <ytlib/profiling/profiler.h>
#include <ytlib/profiling/timing.h>

namespace NYT {
namespace NChunkServer {

using namespace NCellMaster;
using namespace NProto;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ChunkServer");
static NProfiling::TProfiler Profiler("/chunk_server");

////////////////////////////////////////////////////////////////////////////////

TJobScheduler::TJobScheduler(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap,
    TChunkPlacementPtr chunkPlacement,
    THolderLeaseTrackerPtr holderLeaseTracker)
    : Config(config)
    , Bootstrap(bootstrap)
    , ChunkPlacement(chunkPlacement)
    , HolderLeaseTracker(holderLeaseTracker)
    , ChunkRefreshDelay(DurationToCpuDuration(config->ChunkRefreshDelay))
{
    YASSERT(config);
    YASSERT(bootstrap);
    YASSERT(chunkPlacement);
    YASSERT(holderLeaseTracker);

    ScheduleNextRefresh();
}

void TJobScheduler::ScheduleJobs(
    const THolder& holder,
    const yvector<TJobInfo>& runningJobs,
    yvector<TJobStartInfo>* jobsToStart,
    yvector<TJobStopInfo>* jobsToStop)
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

    if (IsEnabled()) {
        ScheduleNewJobs(
            holder,
            Max(0, Config->Jobs->MaxReplicationFanOut - replicationJobCount),
            Max(0, Config->Jobs->MaxRemovalJobsPerHolder - removalJobCount),
            jobsToStart);
    }
}

void TJobScheduler::OnHolderRegistered(const THolder& holder)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YVERIFY(HolderInfoMap.insert(MakePair(holder.GetId(), THolderInfo())).second);

    FOREACH(auto& chunk, holder.StoredChunks()) {
        ScheduleChunkRefresh(chunk->GetId());
    }
}

void TJobScheduler::OnHolderUnregistered(const THolder& holder)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YVERIFY(HolderInfoMap.erase(holder.GetId()) == 1);
}

void TJobScheduler::ScheduleChunkRemoval(const THolder& holder, const TChunkId& chunkId)
{
    auto& holderInfo = GetHolderInfo(holder.GetId());
    holderInfo.ChunksToReplicate.erase(chunkId);
    holderInfo.ChunksToRemove.insert(chunkId);
}

void TJobScheduler::ProcessExistingJobs(
    const THolder& holder,
    const yvector<TJobInfo>& runningJobs,
    yvector<TJobStopInfo>* jobsToStop,
    int* replicationJobCount,
    int* removalJobCount)
{
    *replicationJobCount = 0;
    *removalJobCount = 0;

    yhash_set<TJobId> runningJobIds;

    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH(const auto& jobInfo, runningJobs) {
        auto jobId = TJobId::FromProto(jobInfo.job_id());
        runningJobIds.insert(jobId);
        const auto* job = chunkManager->FindJob(jobId);

        if (!job) {
            LOG_WARNING("Stopping unknown or obsolete job (JobId: %s, Address: %s, HolderId: %d)",
                ~jobId.ToString(),
                ~holder.GetAddress(),
                holder.GetId());
            TJobStopInfo stopInfo;
            *stopInfo.mutable_job_id() = jobId.ToProto();
            jobsToStop->push_back(stopInfo);
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
                LOG_INFO("Job is running (JobId: %s, Address: %s, HolderId: %d)",
                    ~jobId.ToString(),
                    ~holder.GetAddress(),
                    holder.GetId());

                if (TInstant::Now() - job->GetStartTime() > Config->Jobs->JobTimeout) {
                    TJobStopInfo stopInfo;
                    *stopInfo.mutable_job_id() = jobId.ToProto();
                    jobsToStop->push_back(stopInfo);

                    LOG_WARNING("Job timed out (JobId: %s, Address: %s, HolderId: %d, Duration: %d ms)",
                        ~jobId.ToString(),
                        ~holder.GetAddress(),
                        holder.GetId(),
                        static_cast<i32>((TInstant::Now() - job->GetStartTime()).MilliSeconds()));
                }
                break;

            case EJobState::Completed:
            case EJobState::Failed: {
                TJobStopInfo stopInfo;
                *stopInfo.mutable_job_id() = jobId.ToProto();
                jobsToStop->push_back(stopInfo);

                ScheduleChunkRefresh(job->GetChunkId());

                LOG_INFO("Job %s (JobId: %s, Address: %s, HolderId: %d)",
                    jobState == EJobState::Completed ? "completed" : "failed",
                    ~jobId.ToString(),
                    ~holder.GetAddress(),
                    holder.GetId());
                break;
            }

            default:
                YUNREACHABLE();
        }
    }

    // Check for missing jobs
    FOREACH (auto job, holder.Jobs()) {
        auto jobId = job->GetId();
        if (runningJobIds.find(jobId) == runningJobIds.end()) {
            TJobStopInfo stopInfo;
            *stopInfo.mutable_job_id() = jobId.ToProto();
            jobsToStop->push_back(stopInfo);

            LOG_WARNING("Job is missing (JobId: %s, Address: %s, HolderId: %d)",
                ~jobId.ToString(),
                ~holder.GetAddress(),
                holder.GetId());
        }
    }
}

bool TJobScheduler::IsRefreshScheduled(const TChunkId& chunkId)
{
    return RefreshSet.find(chunkId) != RefreshSet.end();
}

TJobScheduler::EScheduleFlags TJobScheduler::ScheduleReplicationJob(
    const THolder& sourceHolder,
    const TChunkId& chunkId,
    yvector<TJobStartInfo>* jobsToStart)
{
    auto chunkManager = Bootstrap->GetChunkManager();
    const auto* chunk = chunkManager->FindChunk(chunkId);
    if (!chunk) {
        LOG_TRACE("Chunk we're about to replicate is missing (ChunkId: %s, Address: %s, HolderId: %d)",
            ~chunkId.ToString(),
            ~sourceHolder.GetAddress(),
            sourceHolder.GetId());
        return EScheduleFlags::Purged;
    }

    if (IsRefreshScheduled(chunkId)) {
        LOG_TRACE("Chunk we're about to replicate is scheduled for another refresh (ChunkId: %s, Address: %s, HolderId: %d)",
            ~chunkId.ToString(),
            ~sourceHolder.GetAddress(),
            sourceHolder.GetId());
        return EScheduleFlags::Purged;
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
        LOG_TRACE("Chunk we're about to replicate has enough replicas (ChunkId: %s, Address: %s, HolderId: %d)",
            ~chunkId.ToString(),
            ~sourceHolder.GetAddress(),
            sourceHolder.GetId());
        return EScheduleFlags::Purged;
    }

    auto targets = ChunkPlacement->GetReplicationTargets(*chunk, requestedCount);
    if (targets.empty()) {
        LOG_TRACE("No suitable target holders for replication (ChunkId: %s, Address: %s, HolderId: %d)",
            ~chunkId.ToString(),
            ~sourceHolder.GetAddress(),
            sourceHolder.GetId());
        return EScheduleFlags::None;
    }

    yvector<Stroka> targetAddresses;
    FOREACH (auto holderId, targets) {
        const auto& holder = chunkManager->GetHolder(holderId);
        targetAddresses.push_back(holder.GetAddress());
        ChunkPlacement->OnSessionHinted(holder);
    }

    auto jobId = TJobId::Create();
    TJobStartInfo startInfo;
    *startInfo.mutable_job_id() = jobId.ToProto();
    startInfo.set_type(EJobType::Replicate);
    *startInfo.mutable_chunk_id() = chunkId.ToProto();
    ToProto(startInfo.mutable_target_addresses(), targetAddresses);
    startInfo.set_start_time(TInstant::Now().GetValue());
    jobsToStart->push_back(startInfo);

    LOG_DEBUG("Chunk replication scheduled (ChunkId: %s, Address: %s, HolderId: %d, JobId: %s, TargetAddresses: [%s])",
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

TJobScheduler::EScheduleFlags TJobScheduler::ScheduleBalancingJob(
    const THolder& sourceHolder,
    const TChunkId& chunkId,
    yvector<TJobStartInfo>* jobsToStart)
{
    auto chunkManager = Bootstrap->GetChunkManager();
    auto* chunk = &chunkManager->GetChunk(chunkId);

    if (IsRefreshScheduled(chunkId)) {
        LOG_DEBUG("Postponed chunk balancing until another refresh (ChunkId: %s, Address: %s, HolderId: %d)",
            ~chunkId.ToString(),
            ~sourceHolder.GetAddress(),
            sourceHolder.GetId());
        return EScheduleFlags::None;
    }

    double maxFillCoeff =
        ChunkPlacement->GetFillCoeff(sourceHolder) -
        Config->Jobs->MinBalancingFillCoeffDiff;
    auto targetHolderId = ChunkPlacement->GetBalancingTarget(chunk, maxFillCoeff);
    if (targetHolderId == InvalidHolderId) {
        LOG_DEBUG("No suitable target holders for balancing (ChunkId: %s, Address: %s, HolderId: %d)",
            ~chunkId.ToString(),
            ~sourceHolder.GetAddress(),
            sourceHolder.GetId());
        return EScheduleFlags::None;
    }

    const auto& targetHolder = chunkManager->GetHolder(targetHolderId);
    ChunkPlacement->OnSessionHinted(targetHolder);
    
    auto jobId = TJobId::Create();
    TJobStartInfo startInfo;
    *startInfo.mutable_job_id() = jobId.ToProto();
    startInfo.set_type(EJobType::Replicate);
    *startInfo.mutable_chunk_id() = chunkId.ToProto();
    startInfo.add_target_addresses(targetHolder.GetAddress());
    startInfo.set_start_time(TInstant::Now().GetValue());
    jobsToStart->push_back(startInfo);

    LOG_DEBUG("Chunk balancing scheduled (ChunkId: %s, Address: %s, HolderId: %d, JobId: %s, TargetAddress: %s)",
        ~chunkId.ToString(),
        ~sourceHolder.GetAddress(),
        sourceHolder.GetId(),
        ~jobId.ToString(),
        ~targetHolder.GetAddress());

    // TODO: flagged enums
    return (EScheduleFlags) (EScheduleFlags::Purged | EScheduleFlags::Scheduled);
}

TJobScheduler::EScheduleFlags TJobScheduler::ScheduleRemovalJob(
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
    *startInfo.mutable_job_id() = jobId.ToProto();
    startInfo.set_type(EJobType::Remove);
    *startInfo.mutable_chunk_id() = chunkId.ToProto();
    startInfo.set_start_time(TInstant::Now().GetValue());
    jobsToStart->push_back(startInfo);

    LOG_DEBUG("Removal job scheduled (ChunkId: %s, Address: %s, HolderId: %d, JobId: %s)",
        ~chunkId.ToString(),
        ~holder.GetAddress(),
        holder.GetId(),
        ~jobId.ToString());

    // TODO: flagged enums
    return (EScheduleFlags) (EScheduleFlags::Purged | EScheduleFlags::Scheduled);
}

void TJobScheduler::ScheduleNewJobs(
    const THolder& holder,
    int maxReplicationJobsToStart,
    int maxRemovalJobsToStart,
    yvector<TJobStartInfo>* jobsToStart)
{
    auto* holderInfo = FindHolderInfo(holder.GetId());
    if (!holderInfo)
        return;

    // Schedule replication jobs.
    if (maxReplicationJobsToStart > 0) {
        auto& chunksToReplicate = holderInfo->ChunksToReplicate;
        auto it = chunksToReplicate.begin();
        while (it != chunksToReplicate.end()) {
            auto jt = it;
            ++jt;
            const auto& chunkId = *it;
            if (maxReplicationJobsToStart == 0) {
                break;
            }
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
        ChunkPlacement->GetFillCoeff(holder) > Config->Jobs->MinBalancingFillCoeff)
    {
        auto chunksToBalance = ChunkPlacement->GetBalancingChunks(holder, maxReplicationJobsToStart);
        if (!chunksToBalance.empty()) {
            LOG_DEBUG("Holder is eligible for balancing (Address: %s, HolderId: %d, ChunkIds: [%s])",
                ~holder.GetAddress(),
                holder.GetId(),
                ~JoinToString(chunksToBalance));

            FOREACH (const auto& chunkId, chunksToBalance) {
                if (maxReplicationJobsToStart == 0) {
                    break;
                }
                auto flags = ScheduleBalancingJob(holder, chunkId, jobsToStart);
                if (flags & EScheduleFlags::Scheduled) {
                    --maxReplicationJobsToStart;
                }
            }
        }
    }

    // Schedule removal jobs.
    if (maxRemovalJobsToStart > 0) {
        auto& chunksToRemove = holderInfo->ChunksToRemove;
        auto it = chunksToRemove.begin();
        while (it != chunksToRemove.end()) {
            const auto& chunkId = *it;
            auto jt = it;
            ++jt;
            if (maxRemovalJobsToStart == 0) {
                break;
            }
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

void TJobScheduler::GetReplicaStatistics(
    const TChunk& chunk,
    int* desiredCount,
    int* storedCount,
    int* cachedCount,
    int* plusCount,
    int* minusCount)
{
    *desiredCount = GetDesiredReplicaCount(chunk);
    *storedCount = static_cast<int>(chunk.StoredLocations().size());
    *cachedCount = !~chunk.CachedLocations() ? 0 : static_cast<int>(chunk.CachedLocations()->size());
    *plusCount = 0;
    *minusCount = 0;

    if (*storedCount == 0) {
        return;
    }

    auto chunkManager = Bootstrap->GetChunkManager();
    const auto* jobList = chunkManager->FindJobList(chunk.GetId());
    if (jobList) {
        yhash_set<Stroka> storedAddresses(*storedCount);
        FOREACH(auto holderId, chunk.StoredLocations()) {
            const auto& holder = chunkManager->GetHolder(holderId);
            storedAddresses.insert(holder.GetAddress());
        }

        FOREACH(auto& job, jobList->Jobs()) {
            switch (job->GetType()) {
                case EJobType::Replicate: {
                    FOREACH(const auto& address, job->TargetAddresses()) {
                        if (storedAddresses.find(address) == storedAddresses.end()) {
                            ++*plusCount;
                        }
                    }
                    break;
                }

                case EJobType::Remove:
                    if (storedAddresses.find(job->GetRunnerAddress()) != storedAddresses.end()) {
                        ++*minusCount;
                    }
                    break;

                default:
                    YUNREACHABLE();
                }
        }
    }
}

int TJobScheduler::GetDesiredReplicaCount(const TChunk& chunk)
{
    // TODO(babenko): make configurable
    UNUSED(chunk);
    return 3;
}

void TJobScheduler::Refresh(const TChunk& chunk)
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

    auto chunkManager = Bootstrap->GetChunkManager();
    if (storedCount == 0) {
        LostChunkIds_.insert(chunkId);

        LOG_TRACE("Chunk is lost (ChunkId: %s, ReplicaCount: %s, DesiredReplicaCount: %d)",
            ~chunk.GetId().ToString(),
            ~replicaCountStr,
            desiredCount);
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
            const auto& holder = chunkManager->GetHolder(holderId);
            holderAddresses.push_back(holder.GetAddress());
        }

        LOG_DEBUG("Chunk is over-replicated, removal is scheduled at [%s] (ChunkId: %s, ReplicaCount: %s, DesiredReplicaCount: %d)",
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
        const auto& holder = chunkManager->GetHolder(holderId);

        holderInfo.ChunksToReplicate.insert(chunk.GetId());

        LOG_DEBUG("Chunk is under-replicated, replication is scheduled at %s (ChunkId: %s, ReplicaCount: %s, DesiredReplicaCount: %d)",
            ~holder.GetAddress(),
            ~chunk.GetId().ToString(),
            ~replicaCountStr,
            desiredCount);
    } else {
        LOG_TRACE("Chunk is OK (ChunkId: %s, ReplicaCount: %s, DesiredReplicaCount: %d)",
            ~chunk.GetId().ToString(),
            ~replicaCountStr,
            desiredCount);
    }
 }

void TJobScheduler::ScheduleChunkRefresh(const TChunkId& chunkId)
{
    if (RefreshSet.find(chunkId) != RefreshSet.end())
        return;

    TRefreshEntry entry;
    entry.ChunkId = chunkId;
    entry.When = GetCpuInstant() + ChunkRefreshDelay;
    RefreshList.push_back(entry);
    RefreshSet.insert(chunkId);
}

void TJobScheduler::RefreshAllChunks()
{
    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (auto* chunk, chunkManager->GetChunks()) {
        Refresh(*chunk);
    }
}

void TJobScheduler::ScheduleNextRefresh()
{
    auto context = Bootstrap->GetMetaStateManager()->GetEpochContext();
    if (!context)
        return;
    TDelayedInvoker::Submit(
        BIND(&TJobScheduler::OnRefresh, MakeStrong(this))
        .Via(
            Bootstrap->GetStateInvoker(EStateThreadQueue::ChunkRefresh),
            context),
        Config->ChunkRefreshQuantum);
}

void TJobScheduler::OnRefresh()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    PROFILE_TIMING ("incremental_chunk_refresh_time") {
        auto chunkManager = Bootstrap->GetChunkManager();
        auto now = GetCpuInstant();
        for (int i = 0; i < Config->MaxChunksPerRefresh; ++i) {
            if (RefreshList.empty())
                break;

            const auto& entry = RefreshList.front();
            if (entry.When > now)
                break;

            auto* chunk = chunkManager->FindChunk(entry.ChunkId);
            if (chunk) {
                Refresh(*chunk);
            }

            YVERIFY(RefreshSet.erase(entry.ChunkId) == 1);
            RefreshList.pop_front();
        }
    }

    ScheduleNextRefresh();
}

TJobScheduler::THolderInfo* TJobScheduler::FindHolderInfo(THolderId holderId)
{
    auto it = HolderInfoMap.find(holderId);
    return it == HolderInfoMap.end() ? NULL : &it->second;
}

TJobScheduler::THolderInfo& TJobScheduler::GetHolderInfo(THolderId holderId)
{
    auto it = HolderInfoMap.find(holderId);
    YASSERT(it != HolderInfoMap.end());
    return it->second;
}

bool TJobScheduler::IsEnabled()
{
    // This method also logs state changes.

    auto config = Config->Jobs;
    if (config->MinOnlineHolderCount) {
        int needOnline = config->MinOnlineHolderCount.Get();
        int gotOnline = HolderLeaseTracker->GetOnlineHolderCount();
        if (gotOnline < needOnline) {
            if (!LastEnabled || LastEnabled.Get()) {
                LOG_INFO("Too few online holders, job scheduler disabled: needed >= %d but got %d",
                    needOnline,
                    gotOnline);
                LastEnabled = false;
            }
            return false;
        }
    }

    if (config->MaxLostChunkFraction)
    {
        auto chunkManager = Bootstrap->GetChunkManager();
        double needFraction = config->MaxLostChunkFraction.Get();
        double gotFraction = (double) chunkManager->LostChunkIds().size() / chunkManager->GetChunkCount();
        if (gotFraction > needFraction) {
            if (!LastEnabled || LastEnabled.Get()) {
                LOG_INFO("Too many lost chunks, job scheduler disabled: needed <= %lf but got %lf",
                    needFraction,
                    gotFraction);
                LastEnabled = false;
            }
            return false;
        }
    }

    if (!LastEnabled || !LastEnabled.Get()) {
        LOG_INFO("Job scheduler enabled");
        LastEnabled = true;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
