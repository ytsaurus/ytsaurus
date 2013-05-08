#include "stdafx.h"
#include "chunk_replicator.h"
#include "chunk_placement.h"
#include "job.h"
#include "chunk.h"
#include "chunk_list.h"
#include "chunk_tree_traversing.h"
#include "private.h"

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/string.h>
#include <ytlib/misc/small_vector.h>
#include <ytlib/misc/protobuf_helpers.h>

#include <ytlib/node_tracker_client/node_directory.h>
#include <ytlib/node_tracker_client/helpers.h>

#include <ytlib/erasure/codec.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/profiling/profiler.h>
#include <ytlib/profiling/timing.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/config.h>
#include <server/cell_master/meta_state_facade.h>

#include <server/chunk_server/chunk_manager.h>
#include <server/chunk_server/node_directory_builder.h>

#include <server/node_tracker_server/node_tracker.h>
#include <server/node_tracker_server/node.h>

#include <server/cypress_server/node.h>

namespace NYT {
namespace NChunkServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NChunkServer::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;
static NProfiling::TProfiler& Profiler = ChunkServerProfiler;

////////////////////////////////////////////////////////////////////////////////

TChunkReplicator::TChunkStatistics::TChunkStatistics()
{
    memset(ReplicaCount, 0, sizeof (ReplicaCount));
}

////////////////////////////////////////////////////////////////////////////////

TChunkReplicator::TRefreshEntry::TRefreshEntry()
    : Chunk(nullptr)
{ }

////////////////////////////////////////////////////////////////////////////////

TChunkReplicator::TChunkReplicator(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap,
    TChunkPlacementPtr chunkPlacement)
    : Config(config)
    , Bootstrap(bootstrap)
    , ChunkPlacement(chunkPlacement)
    , ChunkRefreshDelay(DurationToCpuDuration(config->ChunkRefreshDelay))
{
    YCHECK(config);
    YCHECK(bootstrap);
    YCHECK(chunkPlacement);
}

void TChunkReplicator::Initialize()
{
    RefreshInvoker = New<TPeriodicInvoker>(
        Bootstrap->GetMetaStateFacade()->GetEpochInvoker(EStateThreadQueue::ChunkMaintenance),
        BIND(&TChunkReplicator::OnRefresh, MakeWeak(this)),
        Config->ChunkRefreshPeriod);
    RefreshInvoker->Start();

    RFUpdateInvoker = New<TPeriodicInvoker>(
        Bootstrap->GetMetaStateFacade()->GetEpochInvoker(EStateThreadQueue::ChunkMaintenance),
        BIND(&TChunkReplicator::OnRFUpdate, MakeWeak(this)),
        Config->ChunkRFUpdatePeriod,
        EPeriodicInvokerMode::Manual);
    RFUpdateInvoker->Start();

    auto nodeTracker = Bootstrap->GetNodeTracker();
    FOREACH (auto* node, nodeTracker->GetNodes()) {
        OnNodeRegistered(node);
    }

    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (auto* chunk, chunkManager->GetChunks()) {
        ScheduleChunkRefresh(chunk);
        ScheduleRFUpdate(chunk);
    }
}

void TChunkReplicator::Finalize()
{
    auto nodeTracker = Bootstrap->GetNodeTracker();
    FOREACH (auto* node, nodeTracker->GetNodes()) {
        node->SafelyStoredReplicas().clear();
    }
}

void TChunkReplicator::TouchChunk(TChunk* chunk)
{
    auto repairIt = chunk->GetRepairQueueIterator();
    if (repairIt != TChunkRepairQueueIterator()) {
        RepairQueue.erase(repairIt);
        auto newRepairIt = RepairQueue.insert(RepairQueue.begin(), chunk);
        chunk->SetRepairQueueIterator(newRepairIt);
    }
}

TJobPtr TChunkReplicator::FindJob(const TJobId& id)
{
    auto it = JobMap.find(id);
    return it == JobMap.end() ? nullptr : it->second;
}

TJobListPtr TChunkReplicator::FindJobList(const TChunkId& id)
{
    auto it = JobListMap.find(id);
    return it == JobListMap.end() ? nullptr : it->second;
}

EChunkStatus TChunkReplicator::ComputeChunkStatus(TChunk* chunk)
{
    auto statistics = ComputeChunkStatistics(chunk);
    return statistics.Status;
}

TChunkReplicator::TChunkStatistics TChunkReplicator::ComputeChunkStatistics(TChunk* chunk)
{
    return chunk->IsErasure()
        ? ComputeErasureChunkStatistics(chunk)
        : ComputeRegularChunkStatistics(chunk);
}

TChunkReplicator::TChunkStatistics TChunkReplicator::ComputeRegularChunkStatistics(TChunk* chunk)
{
    TChunkStatistics result;

    int replicaCount = 0;
    int replicationFactor = chunk->GetReplicationFactor();

    FOREACH (auto replica, chunk->StoredReplicas()) {
        if (!IsReplicaDecommissioned(replica)) {
            ++replicaCount;
        }
    }

    if (replicaCount == 0 && chunk->StoredReplicas().empty()) {
        result.Status |= EChunkStatus::Lost;
    } else if (replicaCount > replicationFactor) {
        result.Status |= EChunkStatus::Overreplicated;
    } else if (replicaCount < replicationFactor) {
        result.Status |= EChunkStatus::Underreplicated;
    }

    if (replicaCount >= replicationFactor) {
        result.Status |= EChunkStatus::Safe;
    }

    result.ReplicaCount[0] = replicaCount;

    return result;
}

TChunkReplicator::TChunkStatistics TChunkReplicator::ComputeErasureChunkStatistics(TChunk* chunk)
{
    TChunkStatistics result;

    // Check data and parity parts.
    auto* codec = NErasure::GetCodec(chunk->GetErasureCodec());
    int totalPartCount = codec->GetTotalPartCount();
    int dataPartCount = codec->GetDataPartCount();
    int parityPartCount = codec->GetParityPartCount();

    auto missingIndexes = NErasure::TPartIndexSet((1 << totalPartCount) - 1);  // ignores decommissioned replicas
    auto lostIndexes    = NErasure::TPartIndexSet((1 << totalPartCount) - 1);  // takes decommissioned replicas into account
    TSmallVector<int, NErasure::MaxTotalPartCount> overreplicatedIndexes;
    FOREACH (auto replica, chunk->StoredReplicas()) {
        int index = replica.GetIndex();
        if (!IsReplicaDecommissioned(replica)) {
            if (++result.ReplicaCount[index] > 1) {
                result.Status |= EChunkStatus::Overreplicated;
            }
            missingIndexes.reset(index);
        }
        lostIndexes.reset(index);
    }


    auto dataIndexes = NErasure::TPartIndexSet((1 << dataPartCount) - 1);
    if ((missingIndexes & dataIndexes).any()) {
        result.Status |= EChunkStatus::DataMissing;
    }

    auto parityIndexes = NErasure::TPartIndexSet(((1 << parityPartCount) - 1) << dataPartCount);
    if ((missingIndexes & parityIndexes).any()) {
        result.Status |= EChunkStatus::ParityMissing;
    }

    if (lostIndexes.any()) {
        // Something is damaged.
        if (!codec->CanRepair(lostIndexes)) {
            result.Status |= EChunkStatus::Lost;
        }
    }

    if (!missingIndexes.any()) {
        result.Status |= EChunkStatus::Safe;
    }

    return result;
}

void TChunkReplicator::ScheduleJobs(
    TNode* node,
    const std::vector<TJobPtr>& runningJobs,
    std::vector<TJobPtr>* jobsToStart,
    std::vector<TJobPtr>* jobsToAbort,
    std::vector<TJobPtr>* jobsToRemove)
{
    ProcessExistingJobs(
        node,
        runningJobs,
        jobsToAbort,
        jobsToRemove);

    if (IsEnabled()) {
        ScheduleNewJobs(
            node,
            jobsToStart,
            jobsToAbort);
    }

    FOREACH (auto job, *jobsToStart) {
        RegisterJob(job);
    }

    FOREACH (auto job, *jobsToRemove) {
        UnregisterJob(job);
    }
}

void TChunkReplicator::OnNodeRegistered(TNode* node)
{
    node->ChunkRemovalQueue().clear();

    FOREACH (auto& queue, node->ChunkReplicationQueues()) {
        queue.clear();
    }

    ScheduleNodeRefresh(node);
}

void TChunkReplicator::OnNodeUnregistered(TNode* node)
{
    FOREACH (auto job, node->Jobs()) {
        UnregisterJob(
            job,
            EJobUnregisterFlags(EJobUnregisterFlags::UnregisterFromChunk | EJobUnregisterFlags::ScheduleChunkRefresh));
    }
    node->Jobs().clear();
}

void TChunkReplicator::OnChunkDestroyed(TChunk* chunk)
{
    ResetChunkStatus(chunk);

    const auto& chunkId = chunk->GetId();
    auto it = JobListMap.find(chunkId);
    if (it != JobListMap.end()) {
        auto jobList = it->second;
        FOREACH (auto job, jobList->Jobs()) {
            UnregisterJob(job, EJobUnregisterFlags::UnregisterFromNode);
        }
        JobListMap.erase(it);
    }
}

void TChunkReplicator::ScheduleUnknownChunkRemoval(TNode* node, const TChunkId& chunkId)
{
    node->ChunkRemovalQueue().insert(chunkId);
}

void TChunkReplicator::ScheduleChunkRemoval(TNode* node, TChunkPtrWithIndex chunkWithIndex)
{
    auto chunkId = EncodeChunkId(chunkWithIndex);
    node->ChunkRemovalQueue().insert(chunkId);
}

void TChunkReplicator::ProcessExistingJobs(
    TNode* node,
    const std::vector<TJobPtr>& currentJobs,
    std::vector<TJobPtr>* jobsToAbort,
    std::vector<TJobPtr>* jobsToRemove)
{
    const auto& address = node->GetAddress();

    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (const auto& job, currentJobs) {
        if (job->GetType() == EJobType::Foreign)
            continue;

        const auto& jobId = job->GetJobId();
        switch (job->GetState()) {
            case EJobState::Running:
                if (TInstant::Now() - job->GetStartTime() > Config->ChunkReplicator->JobTimeout) {
                    jobsToAbort->push_back(job);
                    LOG_WARNING("Job timed out (JobId: %s, Address: %s, Duration: %s)",
                        ~ToString(jobId),
                        ~address,
                        ~ToString(TInstant::Now() - job->GetStartTime()));
                } else {
                    LOG_INFO("Job is running (JobId: %s, Address: %s)",
                        ~ToString(jobId),
                        ~address);
                }
                break;

            case EJobState::Completed:
            case EJobState::Failed:
            case EJobState::Aborted: {
                jobsToRemove->push_back(job);
                switch (job->GetState()) {
                    case EJobState::Completed:
                        LOG_INFO("Job completed (JobId: %s, Address: %s)",
                            ~ToString(jobId),
                            ~address);
                        break;

                    case EJobState::Failed:
                        LOG_WARNING(job->Error(), "Job failed (JobId: %s, Address: %s)",
                            ~ToString(jobId),
                            ~address);
                        break;

                    case EJobState::Aborted:
                        LOG_WARNING(job->Error(), "Job aborted (JobId: %s, Address: %s)",
                            ~ToString(jobId),
                            ~address);
                        break;

                    default:
                        YUNREACHABLE();
                }
                break;
            }


            default:
                YUNREACHABLE();
        }
    }

    // Check for missing jobs
    yhash_set<TJobPtr> currentJobSet(currentJobs.begin(), currentJobs.end());
    std::vector<TJobPtr> missingJobs;
    FOREACH (const auto& job, node->Jobs()) {
        if (currentJobSet.find(job) == currentJobSet.end()) {
            missingJobs.push_back(job);
            LOG_WARNING("Job is missing (JobId: %s, Address: %s)",
                ~ToString(job->GetJobId()),
                ~address);
        }
    }
    FOREACH (const auto& job, missingJobs) {
        UnregisterJob(job);
    }
}

TChunkReplicator::EJobScheduleFlags TChunkReplicator::ScheduleReplicationJob(
    TNode* sourceNode,
    TChunk* chunk,
    TJobPtr* job)
{
    YCHECK(!chunk->IsErasure());

    const auto& chunkId = chunk->GetId();
    auto chunkManager = Bootstrap->GetChunkManager();

    if (!IsObjectAlive(chunk)) {
        return EJobScheduleFlags::Purged;
    }

    if (chunk->GetRefreshScheduled()) {
        return EJobScheduleFlags::Purged;
    }

    if (HasRunningJobs(chunkId)) {
        return EJobScheduleFlags::Purged;
    }

    auto statistics = ComputeChunkStatistics(chunk);
    int replicasNeeded = chunk->GetReplicationFactor() - statistics.ReplicaCount[0];
    if (replicasNeeded <= 0) {
        return EJobScheduleFlags::Purged;
    }

    auto targets = ChunkPlacement->GetReplicationTargets(chunk, replicasNeeded);
    if (targets.empty()) {
        return EJobScheduleFlags::None;
    }

    std::vector<Stroka> targetAddresses;
    FOREACH (auto* target, targets) {
        ChunkPlacement->OnSessionHinted(target);
        targetAddresses.push_back(target->GetAddress());
    }

    TNodeResources resourceUsage;
    resourceUsage.set_replication_slots(1);
    *job = TJob::CreateReplicate(
        chunkId,
        sourceNode,
        targetAddresses,
        resourceUsage);

    LOG_INFO("Replication job scheduled (JobId: %s, Address: %s, ChunkId: %s, TargetAddresses: [%s])",
        ~ToString((*job)->GetJobId()),
        ~sourceNode->GetAddress(),
        ~ToString(chunkId),
        ~JoinToString(targetAddresses));

    return
        targets.size() == replicasNeeded
        ? EJobScheduleFlags(EJobScheduleFlags::Purged | EJobScheduleFlags::Scheduled)
        : EJobScheduleFlags(EJobScheduleFlags::Scheduled);
}

TChunkReplicator::EJobScheduleFlags TChunkReplicator::ScheduleBalancingJob(
    TNode* sourceNode,
    TChunkPtrWithIndex chunkWithIndex,
    double maxFillCoeff,
    TJobPtr* job)
{
    auto* chunk = chunkWithIndex.GetPtr();
    const auto& chunkId = chunk->GetId();

    if (chunk->GetRefreshScheduled()) {
        return EJobScheduleFlags::Purged;
    }

    auto* targetNode = ChunkPlacement->GetBalancingTarget(chunkWithIndex, maxFillCoeff);
    if (!targetNode) {
        LOG_DEBUG("No suitable target nodes for balancing (ChunkId: %s)",
            ~ToString(chunkWithIndex));
        return EJobScheduleFlags::None;
    }

    ChunkPlacement->OnSessionHinted(targetNode);

    TNodeResources resourceUsage;
    resourceUsage.set_replication_slots(1);
    *job = TJob::CreateReplicate(
        chunkId,
        sourceNode,
        std::vector<Stroka>(1, targetNode->GetAddress()),
        resourceUsage);

    LOG_INFO("Balancing job scheduled (JobId: %s, Address: %s, ChunkId: %s, TargetAddress: %s)",
        ~ToString((*job)->GetJobId()),
        ~sourceNode->GetAddress(),
        ~ToString(chunkId),
        ~targetNode->GetAddress());

    return EJobScheduleFlags(EJobScheduleFlags::Purged | EJobScheduleFlags::Scheduled);
}

TChunkReplicator::EJobScheduleFlags TChunkReplicator::ScheduleRemovalJob(
    TNode* node,
    const TChunkId& chunkId,
    TJobPtr* job)
{
    auto chunkManager = Bootstrap->GetChunkManager();

    auto* chunk = chunkManager->FindChunk(chunkId);
    if (chunk && chunk->GetRefreshScheduled()) {
        return EJobScheduleFlags::Purged;
    }

    // NB: Allow more than one job for dead chunks.
    if (chunk && HasRunningJobs(chunkId)) {
        return EJobScheduleFlags::Purged;
    }

    TNodeResources resourceUsage;
    resourceUsage.set_removal_slots(1);
    *job = TJob::CreateRemove(
        chunkId,
        node,
        resourceUsage);

    LOG_INFO("Removal job scheduled (JobId: %s, Address: %s, ChunkId: %s)",
        ~ToString((*job)->GetJobId()),
        ~node->GetAddress(),
        ~ToString(chunkId));

    return EJobScheduleFlags(EJobScheduleFlags::Purged | EJobScheduleFlags::Scheduled);
}

TChunkReplicator::EJobScheduleFlags TChunkReplicator::ScheduleRepairJob(
    TNode* node,
    TChunk* chunk,
    TJobPtr* job)
{
    const auto& chunkId = chunk->GetId();

    if (!IsObjectAlive(chunk)) {
        return EJobScheduleFlags::Purged;
    }

    if (chunk->GetRefreshScheduled()) {
        return EJobScheduleFlags::Purged;
    }

    if (HasRunningJobs(chunkId)) {
        return EJobScheduleFlags::Purged;
    }

    auto codecId = chunk->GetErasureCodec();
    auto* codec = NErasure::GetCodec(codecId);

    auto totalBlockCount = codec->GetTotalPartCount();

    NErasure::TPartIndexSet replicaIndexSet;
    int erasedIndexCount = totalBlockCount;
    FOREACH (auto replica, chunk->StoredReplicas()) {
        int index = replica.GetIndex();
        if (!replicaIndexSet[index]) {
            replicaIndexSet.set(index);
            --erasedIndexCount;
        }
    }

    NErasure::TPartIndexList erasedIndexList;
    for (int index = 0; index < totalBlockCount; ++index) {
        if (!replicaIndexSet[index]) {
            erasedIndexList.push_back(index);
        }
    }

    auto targets = ChunkPlacement->GetReplicationTargets(chunk, erasedIndexCount);
    if (targets.size() != erasedIndexCount) {
        return EJobScheduleFlags::None;
    }

    std::vector<Stroka> targetAddresses;
    FOREACH (auto* target, targets) {
        ChunkPlacement->OnSessionHinted(target);
        targetAddresses.push_back(target->GetAddress());
    }

    auto miscExt = GetProtoExtension<TMiscExt>(chunk->ChunkMeta().extensions());

    TNodeResources resourceUsage;
    resourceUsage.set_repair_slots(1);
    // ToDo(babenko): use configurable default.
    resourceUsage.set_memory(0); // miscExt.repair_memory_limit()
    *job = TJob::CreateRepair(
        chunkId,
        node,
        targetAddresses,
        resourceUsage);

    LOG_INFO("Repair job scheduled (JobId: %s, Address: %s, ChunkId: %s, TargetAddresses: [%s], ErasedIndexes: [%s])",
        ~ToString((*job)->GetJobId()),
        ~node->GetAddress(),
        ~ToString(chunkId),
        ~JoinToString(targetAddresses),
        ~JoinToString(erasedIndexList));

    return EJobScheduleFlags(EJobScheduleFlags::Purged | EJobScheduleFlags::Scheduled);
}

void TChunkReplicator::ScheduleNewJobs(
    TNode* node,
    std::vector<TJobPtr>* jobsToStart,
    std::vector<TJobPtr>* jobsToAbort)
{
    auto registerJob = [&] (TJobPtr job) {
        jobsToStart->push_back(job);
        node->ResourceUsage() += job->ResourceUsage();
    };

    // Schedule replication jobs.
    FOREACH (auto& queue, node->ChunkReplicationQueues()) {
        auto it = queue.begin();
        while (it != queue.end()) {
            if (node->ResourceUsage().replication_slots() >= node->ResourceLimits().replication_slots())
                break;

            auto jt = it++;
            const auto& chunkId = *jt;

            TJobPtr job;
            auto flags = ScheduleReplicationJob(node, chunkId, &job);

            if (flags & EJobScheduleFlags::Scheduled) {
                registerJob(job);
            }
            if (flags & EJobScheduleFlags::Purged) {
                queue.erase(jt);
            }
        }
    }

    // Schedule repair jobs.
    {
        auto it = RepairQueue.begin();
        while (it != RepairQueue.end()) {
            if (node->ResourceUsage().repair_slots() >= node->ResourceLimits().repair_slots())
                break;

            auto jt = it++;
            auto* chunk = *jt;

            TJobPtr job;
            auto flags = ScheduleRepairJob(node, chunk, &job);

            if (flags & EJobScheduleFlags::Scheduled) {
                registerJob(job);
            }
            if (flags & EJobScheduleFlags::Purged) {
                chunk->SetRepairQueueIterator(TChunkRepairQueueIterator());
                RepairQueue.erase(jt);
            }
        }
    }

    // Schedule removal jobs.
    {
        auto& ChunkRemovalQueue = node->ChunkRemovalQueue();
        auto it = ChunkRemovalQueue.begin();
        while (it != ChunkRemovalQueue.end()) {
            if (node->ResourceUsage().removal_slots() >= node->ResourceLimits().removal_slots())
                break;

            auto jt = it++;
            const auto& chunkId = *jt;

            TJobPtr job;
            auto flags = ScheduleRemovalJob(node, chunkId, &job);

            if (flags & EJobScheduleFlags::Scheduled) {
                registerJob(job);
            }
            if (flags & EJobScheduleFlags::Purged) {
                ChunkRemovalQueue.erase(jt);
            }
        }
    }

    // Schedule balancing jobs.
    double sourceFillCoeff = ChunkPlacement->GetFillCoeff(node);
    double targetFillCoeff = sourceFillCoeff - Config->ChunkReplicator->MinBalancingFillCoeffDiff;
    if (node->ResourceUsage().replication_slots() < node->ResourceLimits().replication_slots() &&
        sourceFillCoeff > Config->ChunkReplicator->MinBalancingFillCoeff &&
        ChunkPlacement->HasBalancingTargets(targetFillCoeff))
    {
        int maxJobs = std::max(0, node->ResourceLimits().replication_slots() - node->ResourceUsage().replication_slots());
        auto chunksToBalance = ChunkPlacement->GetBalancingChunks(node, maxJobs);
        FOREACH (auto chunkWithIndex, chunksToBalance) {
            if (node->ResourceUsage().replication_slots() >= node->ResourceLimits().replication_slots())
                break;

            TJobPtr job;
            auto flags = ScheduleBalancingJob(node, chunkWithIndex, targetFillCoeff, &job);

            if (flags & EJobScheduleFlags::Scheduled) {
                registerJob(job);
            }
        }
    }
}

void TChunkReplicator::RefreshChunk(TChunk* chunk)
{
    const auto& chunkId = chunk->GetId();

    if (!chunk->IsConfirmed()) {
        return;
    }

    if (HasRunningJobs(chunkId)) {
        return;
    }

    ResetChunkStatus(chunk);

    auto statistics = ComputeChunkStatistics(chunk);

    if (statistics.Status & EChunkStatus::Lost) {
        YCHECK(LostChunks_.insert(chunk).second);

        if (chunk->GetVital()) {
            YCHECK(LostVitalChunks_.insert(chunk).second);
        }
    }

    if (statistics.Status & EChunkStatus::Overreplicated) {
        YCHECK(OverreplicatedChunks_.insert(chunk).second);

        for (int index = 0; index < NErasure::MaxTotalPartCount; ++index) {
            if (statistics.ReplicaCount[index] <= 1)
                continue;

            TChunkPtrWithIndex chunkWithIndex(chunk, index);
            auto encodedChunkId = EncodeChunkId(chunkWithIndex);

            int redundantCount = statistics.ReplicaCount[index] - 1;
            auto nodes = ChunkPlacement->GetRemovalTargets(chunkWithIndex, redundantCount);
            FOREACH (auto* node, nodes) {
                YCHECK(node->ChunkRemovalQueue().insert(encodedChunkId).second);
            }
        }
    }

    if (statistics.Status & EChunkStatus::Underreplicated) {
        YCHECK(UnderreplicatedChunks_.insert(chunk).second);

        YCHECK(!chunk->IsErasure());
        auto* node = ChunkPlacement->GetReplicationSource(chunk);

        int priority = std::min(statistics.ReplicaCount[0], ReplicationPriorityCount) - 1;
        YCHECK(node->ChunkReplicationQueues()[priority].insert(chunk).second);
    }

    if (statistics.Status & EChunkStatus::DataMissing) {
        YCHECK(DataMissingChunks_.insert(chunk).second);
    }

    if (statistics.Status & EChunkStatus::ParityMissing) {
        YCHECK(ParityMissingChunks_.insert(chunk).second);
    }

    if ((statistics.Status & EChunkStatus(EChunkStatus::DataMissing | EChunkStatus::ParityMissing)) &&
        !(statistics.Status & EChunkStatus::Lost))
    {
        auto repairIt = RepairQueue.insert(RepairQueue.end(), chunk);
        chunk->SetRepairQueueIterator(repairIt);
    }

    if (statistics.Status & EChunkStatus::Safe) {
        FOREACH (auto nodeWithIndex, chunk->StoredReplicas()) {
            auto* node = nodeWithIndex.GetPtr();
            TChunkPtrWithIndex chunkWithIndex(chunk, nodeWithIndex.GetIndex());
            if (node->GetDecommissioned()) {
                YCHECK(node->SafelyStoredReplicas().insert(chunkWithIndex).second);
            }
        }
    }
}

void TChunkReplicator::ResetChunkStatus(TChunk* chunk)
{
    FOREACH (auto nodeWithIndex, chunk->StoredReplicas()) {
        auto* node = nodeWithIndex.GetPtr();
        TChunkPtrWithIndex chunkWithIndex(chunk, nodeWithIndex.GetIndex());
        FOREACH (auto& queue, node->ChunkReplicationQueues()) {
            queue.erase(chunk);
        }
        auto chunkId = EncodeChunkId(chunkWithIndex);
        node->ChunkRemovalQueue().erase(chunkId);
    }

    LostChunks_.erase(chunk);
    LostVitalChunks_.erase(chunk);
    OverreplicatedChunks_.erase(chunk);

    if (chunk->IsErasure()) {
        DataMissingChunks_.erase(chunk);
        ParityMissingChunks_.erase(chunk);

        auto repairIt = chunk->GetRepairQueueIterator();
        if (repairIt != TChunkRepairQueueIterator()) {
            RepairQueue.erase(repairIt);
            chunk->SetRepairQueueIterator(TChunkRepairQueueIterator());
        }
    } else {
        UnderreplicatedChunks_.erase(chunk);
    }

    FOREACH (auto nodeWithIndex, chunk->StoredReplicas()) {
        auto* node = nodeWithIndex.GetPtr();
        TChunkPtrWithIndex chunkWithIndex(chunk, nodeWithIndex.GetIndex());
        if (node->GetDecommissioned()) {
            node->SafelyStoredReplicas().erase(chunkWithIndex);
        }
    }
}

bool TChunkReplicator::IsReplicaDecommissioned(TNodePtrWithIndex replica)
{
    auto* node = replica.GetPtr();
    return node->GetDecommissioned();
}

bool TChunkReplicator::HasRunningJobs(const TChunkId& chunkId)
{
    auto chunkManager = Bootstrap->GetChunkManager();
    auto jobList = chunkManager->FindJobList(chunkId);
    return jobList && !jobList->Jobs().empty();
}

void TChunkReplicator::ScheduleChunkRefresh(const TChunkId& chunkId)
{
    auto chunkManager = Bootstrap->GetChunkManager();
    auto* chunk = chunkManager->FindChunk(chunkId);
    if (IsObjectAlive(chunk)) {
        ScheduleChunkRefresh(chunk);
    }
}

void TChunkReplicator::ScheduleChunkRefresh(TChunk* chunk)
{
    if (!IsObjectAlive(chunk) || chunk->GetRefreshScheduled())
        return;

    TRefreshEntry entry;
    entry.Chunk = chunk;
    entry.When = GetCpuInstant() + ChunkRefreshDelay;
    RefreshList.push_back(entry);
    chunk->SetRefreshScheduled(true);

    auto objectManager = Bootstrap->GetObjectManager();
    objectManager->LockObject(chunk);
}

void TChunkReplicator::ScheduleNodeRefresh(TNode* node)
{
    FOREACH (auto replica, node->StoredReplicas()) {
        ScheduleChunkRefresh(replica.GetPtr());
    }

    if (!node->GetDecommissioned()) {
        node->SafelyStoredReplicas().clear();
    }
}

void TChunkReplicator::OnRefresh()
{
    if (RefreshList.empty()) {
        return;
    }

    auto objectManager = Bootstrap->GetObjectManager();

    int count = 0;
    PROFILE_TIMING ("/incremental_refresh_time") {
        auto chunkManager = Bootstrap->GetChunkManager();
        auto now = GetCpuInstant();
        for (int i = 0; i < Config->MaxChunksPerRefresh; ++i) {
            if (RefreshList.empty())
                break;

            const auto& entry = RefreshList.front();
            if (entry.When > now)
                break;

            auto* chunk = entry.Chunk;
            RefreshList.pop_front();
            chunk->SetRefreshScheduled(false);
            ++count;

            if (IsObjectAlive(chunk)) {
                RefreshChunk(chunk);
            }

            objectManager->UnlockObject(chunk);
        }
    }

    LOG_DEBUG("Incremental chunk refresh completed, %d chunks processed",
        count);
}

bool TChunkReplicator::IsEnabled()
{
    // This method also logs state changes.

    auto chunkManager = Bootstrap->GetChunkManager();
    auto nodeTracker = Bootstrap->GetNodeTracker();

    auto config = Config->ChunkReplicator;
    if (config->MinOnlineNodeCount) {
        int needOnline = config->MinOnlineNodeCount.Get();
        int gotOnline = nodeTracker->GetOnlineNodeCount();
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

    int chunkCount = chunkManager->GetChunkCount();
    int lostChunkCount = chunkManager->LostChunks().size();
    if (config->MaxLostChunkFraction && chunkCount > 0) {
        double needFraction = config->MaxLostChunkFraction.Get();
        double gotFraction = (double) lostChunkCount / chunkCount;
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

int TChunkReplicator::GetRefreshListSize() const
{
    return static_cast<int>(RefreshList.size());
}

int TChunkReplicator::GetRFUpdateListSize() const
{
    return static_cast<int>(RFUpdateList.size());
}

void TChunkReplicator::ScheduleRFUpdate(TChunkTree* chunkTree)
{
    switch (chunkTree->GetType()) {
        case EObjectType::Chunk:
            ScheduleRFUpdate(chunkTree->AsChunk());
            break;
        case EObjectType::ErasureChunk:
            // Erasure chunks have no RF.
            break;
        case EObjectType::ChunkList:
            ScheduleRFUpdate(chunkTree->AsChunkList());
            break;
        default:
            YUNREACHABLE();
    }
}

void TChunkReplicator::ScheduleRFUpdate(TChunkList* chunkList)
{
    class TVisitor
        : public IChunkVisitor
    {
    public:
        TVisitor(
            NCellMaster::TBootstrap* bootstrap,
            TChunkReplicatorPtr replicator,
            TChunkList* root)
            : Bootstrap(bootstrap)
            , Replicator(std::move(replicator))
            , Root(root)
        { }

        void Run()
        {
            TraverseChunkTree(Bootstrap, this, Root);
        }

    private:
        TBootstrap* Bootstrap;
        TChunkReplicatorPtr Replicator;
        TChunkList* Root;

        virtual bool OnChunk(
            TChunk* chunk,
            const TReadLimit& startLimit,
            const TReadLimit& endLimit) override
        {
            UNUSED(startLimit);
            UNUSED(endLimit);

            Replicator->ScheduleRFUpdate(chunk);
            return true;
        }

        virtual void OnError(const TError& error) override
        {
            LOG_ERROR(error, "Error traversing chunk tree for RF update");
        }

        virtual void OnFinish() override
        { }

    };

    New<TVisitor>(Bootstrap, this, chunkList)->Run();
}

void TChunkReplicator::ScheduleRFUpdate(TChunk* chunk)
{
    if (!IsObjectAlive(chunk) || chunk->GetRFUpdateScheduled())
        return;

    RFUpdateList.push_back(chunk);
    chunk->SetRFUpdateScheduled(true);

    auto objectManager = Bootstrap->GetObjectManager();
    objectManager->LockObject(chunk);
}

void TChunkReplicator::OnRFUpdate()
{
    if (RFUpdateList.empty() ||
        !Bootstrap->GetMetaStateFacade()->GetManager()->HasActiveQuorum())
    {
        RFUpdateInvoker->ScheduleNext();
        return;
    }

    // Extract up to GCObjectsPerMutation objects and post a mutation.
    auto chunkManager = Bootstrap->GetChunkManager();
    auto objectManager = Bootstrap->GetObjectManager();
    TMetaReqUpdateChunkReplicationFactor request;

    PROFILE_TIMING ("/rf_update_time") {
        for (int i = 0; i < Config->MaxChunksPerRFUpdate; ++i) {
            if (RFUpdateList.empty())
                break;

            auto* chunk = RFUpdateList.front();
            RFUpdateList.pop_front();
            chunk->SetRFUpdateScheduled(false);

            if (IsObjectAlive(chunk)) {
                int replicationFactor = ComputeReplicationFactor(chunk);
                if (chunk->GetReplicationFactor() != replicationFactor) {
                    auto* update = request.add_updates();
                    ToProto(update->mutable_chunk_id(), chunk->GetId());
                    update->set_replication_factor(replicationFactor);
                }
            }

            objectManager->UnlockObject(chunk);
        }
    }

    if (request.updates_size() == 0) {
        RFUpdateInvoker->ScheduleNext();
        return;
    }

    LOG_DEBUG("Starting RF update for %d chunks", request.updates_size());

    auto invoker = Bootstrap->GetMetaStateFacade()->GetEpochInvoker();
    chunkManager
        ->CreateUpdateChunkReplicationFactorMutation(request)
        ->OnSuccess(BIND(&TChunkReplicator::OnRFUpdateCommitSucceeded, MakeWeak(this)).Via(invoker))
        ->OnError(BIND(&TChunkReplicator::OnRFUpdateCommitFailed, MakeWeak(this)).Via(invoker))
        ->PostCommit();
}

void TChunkReplicator::OnRFUpdateCommitSucceeded()
{
    LOG_DEBUG("RF update commit succeeded");

    RFUpdateInvoker->ScheduleOutOfBand();
    RFUpdateInvoker->ScheduleNext();
}

void TChunkReplicator::OnRFUpdateCommitFailed(const TError& error)
{
    LOG_WARNING(error, "RF update commit failed");

    RFUpdateInvoker->ScheduleNext();
}

int TChunkReplicator::ComputeReplicationFactor(TChunk* chunk)
{
    int result = 0;

    // Unique number used to distinguish already visited chunk lists.
    auto mark = TChunkList::GenerateVisitMark();

    // BFS queue. Try to avoid allocations.
    TSmallVector<TChunkList*, 64> queue;
    size_t frontIndex = 0;

    auto enqueue = [&] (TChunkList* chunkList) {
        if (chunkList->GetVisitMark() != mark) {
            chunkList->SetVisitMark(mark);
            queue.push_back(chunkList);
        }
    };

    // Put seeds into the queue.
    FOREACH (auto* parent, chunk->Parents()) {
        auto* adjustedParent = FollowParentLinks(parent);
        if (adjustedParent) {
            enqueue(adjustedParent);
        }
    }

    // The main BFS loop.
    while (frontIndex < queue.size()) {
        auto* chunkList = queue[frontIndex++];

        // Examine owners, if any.
        FOREACH (const auto* owningNode, chunkList->OwningNodes()) {
            if (owningNode->IsTrunk()) {
                result = std::max(result, owningNode->GetOwningReplicationFactor());
            }
        }

        // Proceed to parents.
        FOREACH (auto* parent, chunkList->Parents()) {
            auto* adjustedParent = FollowParentLinks(parent);
            if (adjustedParent) {
                enqueue(adjustedParent);
            }
        }
    }

    return result == 0 ? chunk->GetReplicationFactor() : result;
}

TChunkList* TChunkReplicator::FollowParentLinks(TChunkList* chunkList)
{
    while (chunkList->OwningNodes().empty()) {
        const auto& parents = chunkList->Parents();
        size_t parentCount = parents.size();
        if (parentCount == 0) {
            return nullptr;
        }
        if (parentCount > 1) {
            break;
        }
        chunkList = *parents.begin();
    }
    return chunkList;
}

void TChunkReplicator::RegisterJob(TJobPtr job)
{
    const auto& jobId = job->GetJobId();
    const auto& chunkId = job->GetChunkId();

    YCHECK(JobMap.insert(std::make_pair(jobId, job)).second);
    YCHECK(job->GetNode()->Jobs().insert(job).second);

    auto jobList = FindJobList(chunkId);
    if (!jobList) {
        jobList = New<TJobList>(chunkId);
        YCHECK(JobListMap.insert(std::make_pair(chunkId, jobList)).second);
    }
    jobList->AddJob(job);

    LOG_INFO("Job registered (JobId: %s, JobType: %s, Address: %s)",
        ~ToString(job->GetJobId()),
        ~job->GetType().ToString(),
        ~job->GetNode()->GetAddress());
}

void TChunkReplicator::UnregisterJob(TJobPtr job, EJobUnregisterFlags flags)
{
    const auto& chunkId = job->GetChunkId();

    YCHECK(JobMap.erase(job->GetJobId()) == 1);

    if (flags & EJobUnregisterFlags::UnregisterFromNode) {
        YCHECK(job->GetNode()->Jobs().erase(job) == 1);
    }

    if (flags & EJobUnregisterFlags::UnregisterFromChunk) {
        auto jobList = FindJobList(chunkId);
        YCHECK(jobList);
        jobList->RemoveJob(job);
        if (jobList->Jobs().empty()) {
            YCHECK(JobListMap.erase(chunkId) == 1);
        }
    }

    if (flags & EJobUnregisterFlags::ScheduleChunkRefresh) {
        ScheduleChunkRefresh(chunkId);
    }

    LOG_INFO("Job unregistered (JobId: %s, Address: %s)",
        ~ToString(job->GetJobId()),
        ~job->GetNode()->GetAddress());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
