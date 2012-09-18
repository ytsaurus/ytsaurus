#include "stdafx.h"
#include "chunk_replicator.h"
#include "node_lease_tracker.h"
#include "chunk_placement.h"
#include "node.h"
#include "job.h"
#include "chunk.h"
#include "job_list.h"

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/string.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/config.h>
#include <server/cell_master/meta_state_facade.h>

#include <server/chunk_server/chunk_manager.h>

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

TChunkReplicator::TChunkReplicator(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap,
    TChunkPlacementPtr chunkPlacement,
    TNodeLeaseTrackerPtr nodeLeaseTracker)
    : Config(config)
    , Bootstrap(bootstrap)
    , ChunkPlacement(chunkPlacement)
    , NodeLeaseTracker(nodeLeaseTracker)
    , ChunkRefreshDelay(DurationToCpuDuration(config->ChunkRefreshDelay))
{
    YCHECK(config);
    YCHECK(bootstrap);
    YCHECK(chunkPlacement);
    YCHECK(nodeLeaseTracker);

    ScheduleNextRefresh();
}

void TChunkReplicator::ScheduleJobs(
    TDataNode* node,
    const std::vector<TJobInfo>& runningJobs,
    std::vector<TJobStartInfo>* jobsToStart,
    std::vector<TJobStopInfo>* jobsToStop)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    int replicationJobCount;
    int removalJobCount;
    ProcessExistingJobs(
        node,
        runningJobs,
        jobsToStop,
        &replicationJobCount,
        &removalJobCount);

    if (IsEnabled()) {
        ScheduleNewJobs(
            node,
            Max(0, Config->ChunkReplicator->MaxReplicationFanOut - replicationJobCount),
            Max(0, Config->ChunkReplicator->MaxRemovalJobsPerNode - removalJobCount),
            jobsToStart);
    }
}

void TChunkReplicator::OnNodeRegistered(const TDataNode* node)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YCHECK(NodeInfoMap.insert(MakePair(node->GetId(), TNodeInfo())).second);

    FOREACH (const auto* chunk, node->StoredChunks()) {
        ScheduleChunkRefresh(chunk->GetId());
    }
}

void TChunkReplicator::OnNodeUnregistered(const TDataNode* node)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YCHECK(NodeInfoMap.erase(node->GetId()) == 1);
}

void TChunkReplicator::OnChunkRemoved(const TChunk* chunk)
{
    auto chunkId = chunk->GetId();
    LostChunkIds_.erase(chunkId);
    UnderreplicatedChunkIds_.erase(chunkId);
    OverreplicatedChunkIds_.erase(chunkId);
}

void TChunkReplicator::ScheduleChunkRemoval(const TDataNode* node, const TChunkId& chunkId)
{
    auto* nodeInfo = GetNodeInfo(node->GetId());
    nodeInfo->ChunksToReplicate.erase(chunkId);
    nodeInfo->ChunksToRemove.insert(chunkId);
}

void TChunkReplicator::ProcessExistingJobs(
    const TDataNode* node,
    const std::vector<TJobInfo>& runningJobs,
    std::vector<TJobStopInfo>* jobsToStop,
    int* replicationJobCount,
    int* removalJobCount)
{
    using ::ToString;

    *replicationJobCount = 0;
    *removalJobCount = 0;

    yhash_set<TJobId> runningJobIds;

    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (const auto& jobInfo, runningJobs) {
        auto jobId = TJobId::FromProto(jobInfo.job_id());
        runningJobIds.insert(jobId);
        const auto* job = chunkManager->FindJob(jobId);

        if (!job) {
            LOG_WARNING("Stopping unknown or obsolete job %s on %s",
                ~jobId.ToString(),
                ~node->GetAddress());
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
                LOG_INFO("Job %s is running on %s",
                    ~jobId.ToString(),
                    ~node->GetAddress());

                if (TInstant::Now() - job->GetStartTime() > Config->ChunkReplicator->JobTimeout) {
                    TJobStopInfo stopInfo;
                    *stopInfo.mutable_job_id() = jobId.ToProto();
                    jobsToStop->push_back(stopInfo);

                    LOG_WARNING("Job %s has timed out on %s after %s",
                        ~jobId.ToString(),
                        ~node->GetAddress(),
                        ~ToString(TInstant::Now() - job->GetStartTime()));
                }
                break;

            case EJobState::Completed:
            case EJobState::Failed: {
                TJobStopInfo stopInfo;
                *stopInfo.mutable_job_id() = jobId.ToProto();
                jobsToStop->push_back(stopInfo);

                ScheduleChunkRefresh(job->GetChunkId());

                LOG_INFO("Job %s has %s on %s",
                    ~jobId.ToString(),
                    jobState == EJobState::Completed ? "completed" : "failed",
                    ~node->GetAddress());
                break;
            }

            default:
                YUNREACHABLE();
        }
    }

    // Check for missing jobs
    FOREACH (auto job, node->Jobs()) {
        auto jobId = job->GetId();
        if (runningJobIds.find(jobId) == runningJobIds.end()) {
            TJobStopInfo stopInfo;
            *stopInfo.mutable_job_id() = jobId.ToProto();
            jobsToStop->push_back(stopInfo);

            LOG_WARNING("Job %s is missing on %s",
                ~jobId.ToString(),
                ~node->GetAddress());
        }
    }
}

bool TChunkReplicator::IsRefreshScheduled(const TChunkId& chunkId)
{
    return RefreshSet.find(chunkId) != RefreshSet.end();
}

TChunkReplicator::EScheduleFlags TChunkReplicator::ScheduleReplicationJob(
    TDataNode* sourceNode,
    const TChunkId& chunkId,
    std::vector<TJobStartInfo>* jobsToStart)
{
    auto chunkManager = Bootstrap->GetChunkManager();
    auto chunk = chunkManager->FindChunk(chunkId);
    if (!chunk) {
        LOG_TRACE("Chunk %s we're about to replicate is missing on %s",
            ~chunkId.ToString(),
            ~sourceNode->GetAddress());
        return EScheduleFlags::Purged;
    }

    if (IsRefreshScheduled(chunkId)) {
        LOG_TRACE("Chunk %s we're about to replicate is scheduled for another refresh",
            ~chunkId.ToString());
        return EScheduleFlags::Purged;
    }

    auto statistics = GetReplicaStatistics(chunk);

    int replicasNeeded = statistics.ReplicationFactor - (statistics.StoredCount + statistics.PlusCount);
    if (replicasNeeded <= 0) {
        LOG_TRACE("Chunk %s we're about to replicate has enough replicas",
            ~chunkId.ToString());
        return EScheduleFlags::Purged;
    }

    auto targets = ChunkPlacement->GetReplicationTargets(chunk, replicasNeeded);
    if (targets.empty()) {
        LOG_TRACE("No suitable target nodes to replicate chunk %s",
            ~chunkId.ToString());
        return EScheduleFlags::None;
    }

    std::vector<Stroka> targetAddresses;
    FOREACH (auto* node, targets) {
        targetAddresses.push_back(node->GetAddress());
        ChunkPlacement->OnSessionHinted(node);
    }

    auto jobId = TJobId::Create();
    TJobStartInfo startInfo;
    *startInfo.mutable_job_id() = jobId.ToProto();
    startInfo.set_type(EJobType::Replicate);
    *startInfo.mutable_chunk_id() = chunkId.ToProto();
    ToProto(startInfo.mutable_target_addresses(), targetAddresses);
    jobsToStart->push_back(startInfo);

    LOG_DEBUG("Job %s is scheduled on %s: replicate chunk %s to [%s]",
        ~jobId.ToString(),
        ~sourceNode->GetAddress(),
        ~chunkId.ToString(),
        ~JoinToString(targetAddresses));

    return
        targetAddresses.size() == replicasNeeded
        // TODO: flagged enums
        ? (EScheduleFlags) (EScheduleFlags::Purged | EScheduleFlags::Scheduled)
        : (EScheduleFlags) EScheduleFlags::Scheduled;
}

TChunkReplicator::EScheduleFlags TChunkReplicator::ScheduleBalancingJob(
    TDataNode* sourceNode,
    TChunk* chunk,
    std::vector<TJobStartInfo>* jobsToStart)
{
    auto chunkId = chunk->GetId();

    if (IsRefreshScheduled(chunkId)) {
        LOG_DEBUG("Chunk %s we're about to balance is scheduled for another refresh",
            ~chunkId.ToString());
        return EScheduleFlags::None;
    }

    double maxFillCoeff =
        ChunkPlacement->GetFillCoeff(sourceNode) -
        Config->ChunkReplicator->MinBalancingFillCoeffDiff;
    auto targetNode = ChunkPlacement->GetBalancingTarget(chunk, maxFillCoeff);
    if (targetNode == NULL) {
        LOG_DEBUG("No suitable target nodes to balance chunk %s",
            ~chunkId.ToString());
        return EScheduleFlags::None;
    }

    ChunkPlacement->OnSessionHinted(targetNode);
    
    auto jobId = TJobId::Create();
    TJobStartInfo startInfo;
    *startInfo.mutable_job_id() = jobId.ToProto();
    startInfo.set_type(EJobType::Replicate);
    *startInfo.mutable_chunk_id() = chunkId.ToProto();
    startInfo.add_target_addresses(targetNode->GetAddress());
    jobsToStart->push_back(startInfo);

    LOG_DEBUG("Job %s is scheduled on %s: balance chunk %s to [%s]",
        ~jobId.ToString(),
        ~sourceNode->GetAddress(),
        ~chunkId.ToString(),
        ~targetNode->GetAddress());

    // TODO: flagged enums
    return (EScheduleFlags) (EScheduleFlags::Purged | EScheduleFlags::Scheduled);
}

TChunkReplicator::EScheduleFlags TChunkReplicator::ScheduleRemovalJob(
    TDataNode* node,
    const TChunkId& chunkId,
    std::vector<TJobStartInfo>* jobsToStart)
{
    if (IsRefreshScheduled(chunkId)) {
        LOG_DEBUG("Chunk %s we're about to remove is scheduled for another refresh",
            ~chunkId.ToString());
        return EScheduleFlags::None;
    }
    
    auto jobId = TJobId::Create();
    TJobStartInfo startInfo;
    *startInfo.mutable_job_id() = jobId.ToProto();
    startInfo.set_type(EJobType::Remove);
    *startInfo.mutable_chunk_id() = chunkId.ToProto();
    jobsToStart->push_back(startInfo);

    LOG_DEBUG("Job %s is scheduled on %s: chunk %s will be removed",
        ~jobId.ToString(),
        ~node->GetAddress(),
        ~chunkId.ToString());

    // TODO: flagged enums
    return (EScheduleFlags) (EScheduleFlags::Purged | EScheduleFlags::Scheduled);
}

void TChunkReplicator::ScheduleNewJobs(
    TDataNode* node,
    int maxReplicationJobsToStart,
    int maxRemovalJobsToStart,
    std::vector<TJobStartInfo>* jobsToStart)
{
    auto* nodeInfo = FindNodeInfo(node->GetId());
    if (!nodeInfo)
        return;

    // Schedule replication jobs.
    if (maxReplicationJobsToStart > 0) {
        auto& chunksToReplicate = nodeInfo->ChunksToReplicate;
        auto it = chunksToReplicate.begin();
        while (it != chunksToReplicate.end()) {
            auto jt = it;
            ++jt;
            const auto& chunkId = *it;
            if (maxReplicationJobsToStart == 0) {
                break;
            }
            auto flags = ScheduleReplicationJob(node, chunkId, jobsToStart);
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
        ChunkPlacement->GetFillCoeff(node) > Config->ChunkReplicator->MinBalancingFillCoeff)
    {
        auto chunksToBalance = ChunkPlacement->GetBalancingChunks(node, maxReplicationJobsToStart);
        FOREACH (auto* chunk, chunksToBalance) {
            if (maxReplicationJobsToStart == 0) {
                break;
            }
            auto flags = ScheduleBalancingJob(node, chunk, jobsToStart);
            if (flags & EScheduleFlags::Scheduled) {
                --maxReplicationJobsToStart;
            }
        }
    }

    // Schedule removal jobs.
    if (maxRemovalJobsToStart > 0) {
        auto& chunksToRemove = nodeInfo->ChunksToRemove;
        auto it = chunksToRemove.begin();
        while (it != chunksToRemove.end()) {
            const auto& chunkId = *it;
            auto jt = it;
            ++jt;
            if (maxRemovalJobsToStart == 0) {
                break;
            }
            auto flags = ScheduleRemovalJob(node, chunkId, jobsToStart);
            if (flags & EScheduleFlags::Scheduled) {
                --maxRemovalJobsToStart;
            }
            if (flags & EScheduleFlags::Purged) {
                chunksToRemove.erase(it);
            }
            it = jt;
        }
    }
}

TChunkReplicator::TReplicaStatistics TChunkReplicator::GetReplicaStatistics(const TChunk* chunk)
{
    TReplicaStatistics result;

    result.ReplicationFactor = GetReplicationFactor(chunk);
    result.StoredCount = static_cast<int>(chunk->StoredLocations().size());
    result.CachedCount = !~chunk->CachedLocations() ? 0 : static_cast<int>(chunk->CachedLocations()->size());
    result.PlusCount = 0;
    result.MinusCount = 0;

    if (result.StoredCount == 0) {
        return result;
    }

    auto chunkManager = Bootstrap->GetChunkManager();
    const auto* jobList = chunkManager->FindJobList(chunk->GetId());
    if (jobList) {
        yhash_set<Stroka> storedAddresses(result.StoredCount);
        FOREACH (auto nodeId, chunk->StoredLocations()) {
            const auto& node = chunkManager->GetNode(nodeId);
            storedAddresses.insert(node->GetAddress());
        }

        FOREACH (auto& job, jobList->Jobs()) {
            switch (job->GetType()) {
                case EJobType::Replicate: {
                    FOREACH (const auto& address, job->TargetAddresses()) {
                        if (storedAddresses.find(address) == storedAddresses.end()) {
                            ++result.PlusCount;
                        }
                    }
                    break;
                }

                case EJobType::Remove:
                    if (storedAddresses.find(job->GetAddress()) != storedAddresses.end()) {
                        ++result.MinusCount;
                    }
                    break;

                default:
                    YUNREACHABLE();
                }
        }
    }

    return result;
}

Stroka TChunkReplicator::ToString(const TReplicaStatistics& statistics)
{
    return Sprintf("%d+%d+%d-%d",
        statistics.StoredCount,
        statistics.CachedCount,
        statistics.PlusCount,
        statistics.MinusCount);
}

int TChunkReplicator::GetReplicationFactor(const TChunk* chunk)
{
    return chunk->GetReplicationFactor();
}

void TChunkReplicator::Refresh(const TChunk* chunk)
{
    auto statistics = GetReplicaStatistics(chunk);

    FOREACH (auto nodeId, chunk->StoredLocations()) {
        auto* nodeInfo = FindNodeInfo(nodeId);
        if (nodeInfo) {
            nodeInfo->ChunksToReplicate.erase(chunk->GetId());
            nodeInfo->ChunksToRemove.erase(chunk->GetId());
        }
    }
    auto chunkId = chunk->GetId();
    LostChunkIds_.erase(chunkId);
    OverreplicatedChunkIds_.erase(chunkId);
    UnderreplicatedChunkIds_.erase(chunkId);

    auto chunkManager = Bootstrap->GetChunkManager();
    if (statistics.StoredCount == 0) {
        LostChunkIds_.insert(chunkId);

        LOG_TRACE("Chunk %s is lost: %d replicas needed but only %s exist",
            ~chunkId.ToString(),
            statistics.ReplicationFactor,
            ~ToString(statistics));
    } else if (statistics.StoredCount - statistics.MinusCount > statistics.ReplicationFactor) {
        OverreplicatedChunkIds_.insert(chunkId);

        // NB: Never start removal jobs if new replicas are on the way, hence the check plusCount > 0.
        if (statistics.PlusCount > 0) {
            LOG_WARNING("Chunk %s is over-replicated: %s replicas exist but only %d needed, waiting for pending replications to complete",
                ~chunkId.ToString(),
                ~ToString(statistics),
                statistics.ReplicationFactor);
            return;
        }

        int redundantCount = statistics.StoredCount - statistics.MinusCount - statistics.ReplicationFactor;
        auto nodes = ChunkPlacement->GetRemovalTargets(chunk, redundantCount);
        FOREACH (auto* node, nodes) {
            auto* nodeInfo = GetNodeInfo(node->GetId());
            nodeInfo->ChunksToRemove.insert(chunk->GetId());
        }

        std::vector<Stroka> addreses;
        FOREACH (auto node, nodes) {
            addreses.push_back(node->GetAddress());
        }

        LOG_DEBUG("Chunk %s is over-replicated: %s replicas exist but only %d needed, removal is scheduled on [%s]",
            ~chunkId.ToString(),
            ~ToString(statistics),
            statistics.ReplicationFactor,
            ~JoinToString(addreses));
    } else if (statistics.StoredCount + statistics.PlusCount < statistics.ReplicationFactor) {
        UnderreplicatedChunkIds_.insert(chunkId);

        // NB: Never start replication jobs when removal jobs are in progress, hence the check minusCount > 0.
        if (statistics.MinusCount > 0) {
            LOG_DEBUG("Chunk %s is under-replicated: %s replicas exist but %d needed, waiting for pending removals to complete",
                ~chunkId.ToString(),
                ~ToString(statistics),
                statistics.ReplicationFactor);
            return;
        }

        auto* node = ChunkPlacement->GetReplicationSource(chunk);
        auto* nodeInfo = GetNodeInfo(node->GetId());

        nodeInfo->ChunksToReplicate.insert(chunk->GetId());

        LOG_DEBUG("Chunk %s is under-replicated: %s replicas exist but %d needed, replication is scheduled on %s",
            ~chunkId.ToString(),
            ~ToString(statistics),
            statistics.ReplicationFactor,
            ~node->GetAddress());
    } else {
        LOG_TRACE("Chunk %s is OK: %s replicas exist and %d needed",
            ~chunkId.ToString(),
            ~ToString(statistics),
            statistics.ReplicationFactor);
    }
 }

void TChunkReplicator::ScheduleChunkRefresh(const TChunkId& chunkId)
{
    if (RefreshSet.find(chunkId) != RefreshSet.end())
        return;

    TRefreshEntry entry;
    entry.ChunkId = chunkId;
    entry.When = GetCpuInstant() + ChunkRefreshDelay;
    RefreshList.push_back(entry);
    RefreshSet.insert(chunkId);
}

void TChunkReplicator::RefreshAllChunks()
{
    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (auto* chunk, chunkManager->GetChunks()) {
        Refresh(chunk);
    }
}

void TChunkReplicator::ScheduleNextRefresh()
{
    TDelayedInvoker::Submit(
        BIND(&TChunkReplicator::OnRefresh, MakeStrong(this))
            .Via(Bootstrap->GetMetaStateFacade()->GetUnguardedEpochInvoker(EStateThreadQueue::ChunkRefresh)),
        Config->ChunkRefreshPeriod);
}

void TChunkReplicator::OnRefresh()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ScheduleNextRefresh();

    if (RefreshList.empty())
        return;

    LOG_DEBUG("Incremental chunk refresh started");

    int refreshedCount = 0;
    PROFILE_TIMING ("/incremental_chunk_refresh_time") {
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
                Refresh(chunk);
                ++refreshedCount;
            }

            YCHECK(RefreshSet.erase(entry.ChunkId) == 1);
            RefreshList.pop_front();
        }
    }

    LOG_DEBUG("Incremental chunk refresh completed, %d chunks processed",
        refreshedCount);
}

TChunkReplicator::TNodeInfo* TChunkReplicator::FindNodeInfo(TNodeId nodeId)
{
    auto it = NodeInfoMap.find(nodeId);
    return it == NodeInfoMap.end() ? NULL : &it->second;
}

TChunkReplicator::TNodeInfo* TChunkReplicator::GetNodeInfo(TNodeId nodeId)
{
    auto it = NodeInfoMap.find(nodeId);
    YASSERT(it != NodeInfoMap.end());
    return &it->second;
}

bool TChunkReplicator::IsEnabled()
{
    // This method also logs state changes.

    auto config = Config->ChunkReplicator;
    if (config->MinOnlineNodeCount) {
        int needOnline = config->MinOnlineNodeCount.Get();
        int gotOnline = NodeLeaseTracker->GetOnlineNodeCount();
        if (gotOnline < needOnline) {
            if (!LastEnabled || LastEnabled.Get()) {
                LOG_INFO("Chunk replicator disabled: too few online nodes, needed >= %d but got %d",
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
                LOG_INFO("Chunk replicator disabled: too many lost chunks, needed <= %lf but got %lf",
                    needFraction,
                    gotFraction);
                LastEnabled = false;
            }
            return false;
        }
    }

    if (!LastEnabled || !LastEnabled.Get()) {
        LOG_INFO("Chunk replicator enabled");
        LastEnabled = true;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
