#include "stdafx.h"
#include "chunk_replicator.h"
#include "chunk_placement.h"
#include "job.h"
#include "chunk_list.h"
#include "chunk_owner_base.h"
#include "chunk_tree_traversing.h"
#include "private.h"

#include <core/misc/serialize.h>
#include <core/misc/string.h>
#include <core/misc/small_vector.h>
#include <core/misc/protobuf_helpers.h>

#include <ytlib/node_tracker_client/node_directory.h>
#include <ytlib/node_tracker_client/helpers.h>

#include <core/erasure/codec.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <core/profiling/profiler.h>
#include <core/profiling/timing.h>

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
using namespace NNodeTrackerServer;
using namespace NChunkServer::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = ChunkServerLogger;
static auto& Profiler = ChunkServerProfiler;

////////////////////////////////////////////////////////////////////////////////

TChunkReplicator::TJobRequest::TJobRequest(int index, int count)
    : Index(index)
    , Count(count)
{ }

////////////////////////////////////////////////////////////////////////////////

TChunkReplicator::TChunkStatistics::TChunkStatistics()
{
    Zero(ReplicaCount);
    Zero(DecommissionedReplicaCount);
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
    RefreshExecutor = New<TPeriodicExecutor>(
        Bootstrap->GetMetaStateFacade()->GetEpochInvoker(EStateThreadQueue::ChunkMaintenance),
        BIND(&TChunkReplicator::OnRefresh, MakeWeak(this)),
        Config->ChunkRefreshPeriod);
    RefreshExecutor->Start();

    PropertiesUpdateExecutor = New<TPeriodicExecutor>(
        Bootstrap->GetMetaStateFacade()->GetEpochInvoker(EStateThreadQueue::ChunkMaintenance),
        BIND(&TChunkReplicator::OnPropertiesUpdate, MakeWeak(this)),
        Config->ChunkPropertiesUpdatePeriod,
        EPeriodicExecutorMode::Manual);
    PropertiesUpdateExecutor->Start();

    auto nodeTracker = Bootstrap->GetNodeTracker();
    FOREACH (auto* node, nodeTracker->GetNodes()) {
        OnNodeRegistered(node);
    }

    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (auto* chunk, chunkManager->GetChunks()) {
        ScheduleChunkRefresh(chunk);
        SchedulePropertiesUpdate(chunk);
    }
}

void TChunkReplicator::Finalize()
{
    // Clear JobMap, JobListMap, and unregister jobs from the nodes.
    FOREACH (const auto& pair, JobMap) {
        const auto& job = pair.second;
        YCHECK(job->GetNode()->Jobs().erase(job) == 1);
    }
    JobMap.clear();
    JobListMap.clear();
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

TJobListPtr TChunkReplicator::FindJobList(TChunk* chunk)
{
    auto it = JobListMap.find(chunk);
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
    int decommissionedReplicaCount = 0;
    int replicationFactor = chunk->GetReplicationFactor();
    TNodePtrWithIndexList decommissionedReplicas;

    FOREACH (auto replica, chunk->StoredReplicas()) {
        if (IsReplicaDecommissioned(replica)) {
            ++decommissionedReplicaCount;
            decommissionedReplicas.push_back(replica);
        } else {
            ++replicaCount;
        }
    }

    result.ReplicaCount[0] = replicaCount;
    result.DecommissionedReplicaCount[0] = decommissionedReplicaCount;

    if (replicaCount == 0 && decommissionedReplicaCount == 0) {
        result.Status |= EChunkStatus::Lost;
        return result;
    }
    
    if (replicaCount < replicationFactor) {
        result.Status |= EChunkStatus::Underreplicated;
        result.ReplicationRequests.push_back(TJobRequest(0, replicationFactor - replicaCount));
        return result;
    }

    if (replicaCount == replicationFactor && decommissionedReplicaCount > 0) {
        result.Status |= EChunkStatus::Overreplicated;
        result.DecommissionedRemovalRequests.append(decommissionedReplicas.begin(), decommissionedReplicas.end());
        return result;
    }

    if (replicaCount > replicationFactor) {
        result.Status |= EChunkStatus::Overreplicated;
        result.BalancingRemovalRequests.push_back(TJobRequest(0, replicaCount - replicationFactor));
        return result;
    }

    return result;
}

TChunkReplicator::TChunkStatistics TChunkReplicator::ComputeErasureChunkStatistics(TChunk* chunk)
{
    TChunkStatistics result;

    auto* codec = NErasure::GetCodec(chunk->GetErasureCodec());
    int totalPartCount = codec->GetTotalPartCount();
    int dataPartCount = codec->GetDataPartCount();
    TNodePtrWithIndexList decommissionedReplicas[NErasure::MaxTotalPartCount];

    auto mark = TNode::GenerateVisitMark();

    FOREACH (auto replica, chunk->StoredReplicas()) {
        auto* node = replica.GetPtr();
        int index = replica.GetIndex();
        if (IsReplicaDecommissioned(replica) || node->GetVisitMark() == mark) {
            ++result.DecommissionedReplicaCount[index];
            decommissionedReplicas[index].push_back(replica);
        } else {
            ++result.ReplicaCount[index];
        }
        node->SetVisitMark(mark);
    }

    NErasure::TPartIndexSet erasedIndexes;
    for (int index = 0; index < totalPartCount; ++index) {
        int replicaCount = result.ReplicaCount[index];
        int decommissionedReplicaCount = result.DecommissionedReplicaCount[index];
        
        if (replicaCount >= 1 && decommissionedReplicaCount > 0) {
            result.Status |= EChunkStatus::Overreplicated;
            const auto& replicas = decommissionedReplicas[index];
            result.DecommissionedRemovalRequests.append(replicas.begin(), replicas.end());
        }

        if (replicaCount > 1 && decommissionedReplicaCount == 0) {
            result.Status |= EChunkStatus::Overreplicated;
            result.BalancingRemovalRequests.push_back(TJobRequest(index, replicaCount - 1));
        }

        if (replicaCount == 0 && decommissionedReplicaCount > 0) {
            result.Status |= EChunkStatus::Underreplicated;
            result.ReplicationRequests.push_back(TJobRequest(index, 1));
        }
        
        if (replicaCount == 0 && decommissionedReplicaCount == 0) {
            erasedIndexes.set(index);
            if (index < dataPartCount) {
                result.Status |= EChunkStatus::DataMissing;
            } else {
                result.Status |= EChunkStatus::ParityMissing;
            }
        }
    }

    if (!codec->CanRepair(erasedIndexes)) {
        result.Status |= EChunkStatus::Lost;
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
    // NB: Keep existing removal requests to workaround the following scenario:
    // 1) the last strong reference to a chunk is released while some weak references
    //    remain; the chunk becomes a zombie;
    // 2) a node sends a heartbeat reporting addition of the chunk;
    // 3) master receives the heartbeat and puts the chunk into the removal queue
    //    without (sic!) registering a replica;
    // 4) the last weak reference is dropped, the chunk is being removed;
    //    at this point we must preserve its removal request in the queue.  
    RemoveChunkFromQueues(chunk, false);
    CancelChunkJobs(chunk);
}

void TChunkReplicator::ScheduleUnknownChunkRemoval(TNode* node, const TChunkIdWithIndex& chunkIdWithIndex)
{
    node->ChunkRemovalQueue().insert(chunkIdWithIndex);
}

void TChunkReplicator::ScheduleChunkRemoval(TNode* node, TChunkPtrWithIndex chunkWithIndex)
{
    TChunkIdWithIndex chunkIdWithIndex(chunkWithIndex.GetPtr()->GetId(), chunkWithIndex.GetIndex());
    node->ChunkRemovalQueue().insert(chunkIdWithIndex);
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
                if (TInstant::Now() - job->GetStartTime() > Config->JobTimeout) {
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
                UnregisterJob(job);
                break;
            }

            case EJobState::Waiting:
                LOG_INFO("Job is waiting (JobId: %s, Address: %s)",
                    ~ToString(jobId),
                    ~address);
                break;

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
    TChunkPtrWithIndex chunkWithIndex,
    TJobPtr* job)
{
    auto* chunk = chunkWithIndex.GetPtr();
    int index = chunkWithIndex.GetIndex();

    if (!IsObjectAlive(chunk)) {
        return EJobScheduleFlags::Purged;
    }

    if (chunk->GetRefreshScheduled()) {
        return EJobScheduleFlags::Purged;
    }

    if (HasRunningJobs(chunkWithIndex)) {
        return EJobScheduleFlags::Purged;
    }

    int replicationFactor = chunk->GetReplicationFactor();
    auto statistics = ComputeChunkStatistics(chunk);
    int replicaCount = statistics.ReplicaCount[index];
    int decommissionedReplicaCount = statistics.DecommissionedReplicaCount[index];
    if (replicaCount + decommissionedReplicaCount == 0 || replicaCount >= replicationFactor) {
        return EJobScheduleFlags::Purged;
    }

    int replicasNeeded = replicationFactor - replicaCount;
    auto targets = ChunkPlacement->AllocateWriteTargets(
        chunk,
        replicasNeeded,
        EWriteSessionType::Replication);
    if (targets.empty()) {
        return EJobScheduleFlags::None;
    }

    TNodeResources resourceUsage;
    resourceUsage.set_replication_slots(1);

    *job = TJob::CreateReplicate(
        TChunkIdWithIndex(chunk->GetId(), index),
        sourceNode,
        targets,
        resourceUsage);

    LOG_INFO("Replication job scheduled (JobId: %s, Address: %s, ChunkId: %s, TargetAddresses: [%s])",
        ~ToString((*job)->GetJobId()),
        ~sourceNode->GetAddress(),
        ~ToString(chunkWithIndex),
        ~JoinToString(targets, TNodePtrAddressFormatter()));

    return
        targets.size() == replicasNeeded
        ? EJobScheduleFlags(EJobScheduleFlags::Purged | EJobScheduleFlags::Scheduled)
        : EJobScheduleFlags(EJobScheduleFlags::Scheduled);
}

TChunkReplicator::EJobScheduleFlags TChunkReplicator::ScheduleBalancingJob(
    TNode* sourceNode,
    TChunkPtrWithIndex chunkWithIndex,
    double maxFillFactor,
    TJobPtr* job)
{
    TChunkIdWithIndex chunkIdWithIndex(chunkWithIndex.GetPtr()->GetId(), chunkWithIndex.GetIndex());
    auto* chunk = chunkWithIndex.GetPtr();

    if (chunk->GetRefreshScheduled()) {
        return EJobScheduleFlags::Purged;
    }

    auto* target = ChunkPlacement->AllocateBalancingTarget(chunkWithIndex, maxFillFactor);
    if (!target) {
        return EJobScheduleFlags::None;
    }

    TNodeResources resourceUsage;
    resourceUsage.set_replication_slots(1);

    *job = TJob::CreateReplicate(
        chunkIdWithIndex,
        sourceNode,
        TNodeList(1, target),
        resourceUsage);

    LOG_INFO("Balancing job scheduled (JobId: %s, Address: %s, ChunkId: %s, TargetAddress: %s)",
        ~ToString((*job)->GetJobId()),
        ~sourceNode->GetAddress(),
        ~ToString(chunkIdWithIndex),
        ~target->GetAddress());

    return EJobScheduleFlags(EJobScheduleFlags::Purged | EJobScheduleFlags::Scheduled);
}

TChunkReplicator::EJobScheduleFlags TChunkReplicator::ScheduleRemovalJob(
    TNode* node,
    const TChunkIdWithIndex& chunkIdWithIndex,
    TJobPtr* job)
{
    auto chunkManager = Bootstrap->GetChunkManager();
    auto* chunk = chunkManager->FindChunk(chunkIdWithIndex.Id);
    // NB: Allow more than one job for dead chunks.
    if (chunk) {
        if (chunk->GetRefreshScheduled()) {
            return EJobScheduleFlags::Purged;
        }

        if (HasRunningJobs(TChunkPtrWithIndex(chunk, chunkIdWithIndex.Index))) {
            return EJobScheduleFlags::Purged;
        }
    }

    TNodeResources resourceUsage;
    resourceUsage.set_removal_slots(1);

    *job = TJob::CreateRemove(
        chunkIdWithIndex,
        node,
        resourceUsage);

    LOG_INFO("Removal job scheduled (JobId: %s, Address: %s, ChunkId: %s)",
        ~ToString((*job)->GetJobId()),
        ~node->GetAddress(),
        ~ToString(chunkIdWithIndex));

    return EJobScheduleFlags(EJobScheduleFlags::Purged | EJobScheduleFlags::Scheduled);
}

TChunkReplicator::EJobScheduleFlags TChunkReplicator::ScheduleRepairJob(
    TNode* node,
    TChunk* chunk,
    TJobPtr* job)
{
    YCHECK(chunk->IsErasure());

    if (!IsObjectAlive(chunk)) {
        return EJobScheduleFlags::Purged;
    }

    if (chunk->GetRefreshScheduled()) {
        return EJobScheduleFlags::Purged;
    }

    if (HasRunningJobs(chunk)) {
        return EJobScheduleFlags::Purged;
    }

    auto codecId = chunk->GetErasureCodec();
    auto* codec = NErasure::GetCodec(codecId);
    auto totalPartCount = codec->GetTotalPartCount();

    auto statistics = ComputeChunkStatistics(chunk);

    NErasure::TPartIndexList erasedIndexes;
    for (int index = 0; index < totalPartCount; ++index) {
        if (statistics.ReplicaCount[index] == 0 && statistics.DecommissionedReplicaCount[index] == 0) {
            erasedIndexes.push_back(index);
        }
    }

    int erasedIndexCount = static_cast<int>(erasedIndexes.size());
    if (erasedIndexCount == 0) {
        return EJobScheduleFlags::Purged;
    }

    auto targets = ChunkPlacement->AllocateWriteTargets(
        chunk,
        erasedIndexCount,
        EWriteSessionType::Repair);
    if (targets.empty()) {
        return EJobScheduleFlags::None;
    }

    TNodeResources resourceUsage;
    resourceUsage.set_repair_slots(1);
    resourceUsage.set_memory(Config->RepairJobMemoryUsage);

    *job = TJob::CreateRepair(
        chunk->GetId(),
        node,
        targets,
        erasedIndexes,
        resourceUsage);

    LOG_INFO("Repair job scheduled (JobId: %s, Address: %s, ChunkId: %s, TargetAddresses: [%s], ErasedIndexes: [%s])",
        ~ToString((*job)->GetJobId()),
        ~node->GetAddress(),
        ~ToString(chunk->GetId()),
        ~JoinToString(targets, TNodePtrAddressFormatter()),
        ~JoinToString(erasedIndexes));

    return EJobScheduleFlags(EJobScheduleFlags::Purged | EJobScheduleFlags::Scheduled);
}

void TChunkReplicator::ScheduleNewJobs(
    TNode* node,
    std::vector<TJobPtr>* jobsToStart,
    std::vector<TJobPtr>* jobsToAbort)
{
    auto chunkManager = Bootstrap->GetChunkManager();

    const auto& resourceLimits = node->ResourceLimits();
    auto& resourceUsage = node->ResourceUsage();

    i64 runningReplicationSize = 0;
    i64 runningRepairSize = 0;
    auto increaseRunningSizes = [&] (TJobPtr job) {
        auto type = job->GetType();
        if (type != EJobType::ReplicateChunk && type != EJobType::RepairChunk)
            return;

        auto* chunk = chunkManager->FindChunk(job->GetChunkIdWithIndex().Id);
        if (!chunk)
            return;

        i64 size = chunk->ChunkInfo().disk_space();

        // NB: Erasure chunks have replicas much smaller than reported by chunk info.
        auto codecId = chunk->GetErasureCodec();
        if (codecId != NErasure::ECodec::None) {
            auto* codec = NErasure::GetCodec(codecId);
            size /= codec->GetTotalPartCount();
        }

        // XXX(babenko): this static_cast is clearly redundant but required to compile it with VS2010.
        switch (static_cast<int>(type)) {
            case EJobType::ReplicateChunk:
                runningReplicationSize += size;
                break;
            case EJobType::RepairChunk:
                runningRepairSize += size;
                break;
        }
    };

    // Compute current data sizes for running replication and repair jobs.
    FOREACH (auto job, node->Jobs()) {
        increaseRunningSizes(job);
    }

    auto registerJob = [&] (TJobPtr job) {
        jobsToStart->push_back(job);
        RegisterJob(job);
        resourceUsage += job->ResourceUsage();
        increaseRunningSizes(job);
    };

    // Schedule replication jobs.
    FOREACH (auto& queue, node->ChunkReplicationQueues()) {
        auto it = queue.begin();
        while (it != queue.end()) {
            if (resourceUsage.replication_slots() >= resourceLimits.replication_slots())
                break;
            if (runningReplicationSize > Config->MaxReplicationJobsSize)
                break;

            auto jt = it++;
            auto chunkWithIndex = *jt;

            TJobPtr job;
            auto flags = ScheduleReplicationJob(node, chunkWithIndex, &job);

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
            if (resourceUsage.repair_slots() >= resourceLimits.repair_slots())
                break;
            if (runningRepairSize > Config->MaxRepairJobsSize)
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
        auto& removalQueue = node->ChunkRemovalQueue();
        auto it = removalQueue.begin();
        while (it != removalQueue.end()) {
            if (resourceUsage.removal_slots() >= resourceLimits.removal_slots())
                break;

            auto jt = it++;
            const auto& chunkId = *jt;

            TJobPtr job;
            auto flags = ScheduleRemovalJob(node, chunkId, &job);

            if (flags & EJobScheduleFlags::Scheduled) {
                registerJob(job);
            }
            if (flags & EJobScheduleFlags::Purged) {
                removalQueue.erase(jt);
            }
        }
    }

    // Schedule balancing jobs.
    double sourceFillFactor = ChunkPlacement->GetFillFactor(node);
    double targetFillFactor = sourceFillFactor - Config->MinBalancingFillFactorDiff;
    if (resourceUsage.replication_slots() < resourceLimits.replication_slots() &&
        sourceFillFactor > Config->MinBalancingFillFactor &&
        ChunkPlacement->HasBalancingTargets(targetFillFactor))
    {
        int maxJobs = std::max(0, resourceLimits.replication_slots() - resourceUsage.replication_slots());
        auto chunksToBalance = ChunkPlacement->GetBalancingChunks(node, maxJobs);
        FOREACH (auto chunkWithIndex, chunksToBalance) {
            if (resourceUsage.replication_slots() >= resourceLimits.replication_slots())
                break;
            if (runningReplicationSize > Config->MaxReplicationJobsSize)
                break;

            TJobPtr job;
            auto flags = ScheduleBalancingJob(node, chunkWithIndex, targetFillFactor, &job);

            if (flags & EJobScheduleFlags::Scheduled) {
                registerJob(job);
            }
        }
    }
}

void TChunkReplicator::RefreshChunk(TChunk* chunk)
{
    if (!chunk->IsConfirmed())
        return;

    ResetChunkStatus(chunk);

    auto statistics = ComputeChunkStatistics(chunk);

    if (statistics.Status & EChunkStatus::Lost) {
        YCHECK(LostChunks_.insert(chunk).second);
        if (chunk->GetVital() && (chunk->IsErasure() || chunk->GetReplicationFactor() > 1)) {
            YCHECK(LostVitalChunks_.insert(chunk).second);
        }
    }

    if (statistics.Status & EChunkStatus::Overreplicated) {
        YCHECK(OverreplicatedChunks_.insert(chunk).second);
    }

    if (statistics.Status & EChunkStatus::Underreplicated) {
        YCHECK(UnderreplicatedChunks_.insert(chunk).second);
    }

    if (statistics.Status & EChunkStatus::DataMissing) {
        YCHECK(DataMissingChunks_.insert(chunk).second);
    }

    if (statistics.Status & EChunkStatus::ParityMissing) {
        YCHECK(ParityMissingChunks_.insert(chunk).second);
    }

    if (!HasRunningJobs(chunk)) {
        RemoveChunkFromQueues(chunk, true);

        if (statistics.Status & EChunkStatus::Overreplicated) {
            FOREACH (auto nodeWithIndex, statistics.DecommissionedRemovalRequests) {
                int index = nodeWithIndex.GetIndex();
                TChunkIdWithIndex chunkIdWithIndex(chunk->GetId(), index);
                YCHECK(nodeWithIndex.GetPtr()->ChunkRemovalQueue().insert(chunkIdWithIndex).second);
            }

            FOREACH (const auto& request, statistics.BalancingRemovalRequests) {
                int index = request.Index;
                TChunkPtrWithIndex chunkWithIndex(chunk, index);
                TChunkIdWithIndex chunkIdWithIndex(chunk->GetId(), index);

                auto targets = ChunkPlacement->GetRemovalTargets(chunkWithIndex, request.Count);
                FOREACH (auto* target, targets) {
                    YCHECK(target->ChunkRemovalQueue().insert(chunkIdWithIndex).second);
                }
            }
        }

        if (statistics.Status & EChunkStatus::Underreplicated) {
            FOREACH (const auto& request, statistics.ReplicationRequests) {
                int index = request.Index;
                TChunkPtrWithIndex chunkWithIndex(chunk, index);
                TChunkIdWithIndex chunkIdWithIndex(chunk->GetId(), index);

                // Cap replica count minus one against the range [0, ReplicationPriorityCount - 1].
                int replicaCount = statistics.ReplicaCount[index];
                int priority = std::max(std::min(replicaCount - 1, ReplicationPriorityCount - 1), 0);

                FOREACH (auto replica, chunk->StoredReplicas()) {
                    if (replica.GetIndex() == index) {
                        auto* node = replica.GetPtr();
                        YCHECK(node->ChunkReplicationQueues()[priority].insert(chunkWithIndex).second);
                    }
                }
            }
        }

        if ((statistics.Status & EChunkStatus(EChunkStatus::DataMissing | EChunkStatus::ParityMissing)) &&
            !(statistics.Status & EChunkStatus::Lost))
        {
            auto repairIt = RepairQueue.insert(RepairQueue.end(), chunk);
            chunk->SetRepairQueueIterator(repairIt);
        }
    }
}

void TChunkReplicator::ResetChunkStatus(TChunk* chunk)
{
    LostChunks_.erase(chunk);
    LostVitalChunks_.erase(chunk);
    UnderreplicatedChunks_.erase(chunk);
    OverreplicatedChunks_.erase(chunk);

    if (chunk->IsErasure()) {
        DataMissingChunks_.erase(chunk);
        ParityMissingChunks_.erase(chunk);
    }
}

void TChunkReplicator::RemoveChunkFromQueues(TChunk* chunk, bool includingRemovals)
{
    FOREACH (auto nodeWithIndex, chunk->StoredReplicas()) {
        auto* node = nodeWithIndex.GetPtr();
        TChunkPtrWithIndex chunkWithIndex(chunk, nodeWithIndex.GetIndex());
        TChunkIdWithIndex chunkIdWithIndex(chunk->GetId(), nodeWithIndex.GetIndex());
        FOREACH (auto& queue, node->ChunkReplicationQueues()) {
            queue.erase(chunkWithIndex);
        }
        if (includingRemovals) {
            node->ChunkRemovalQueue().erase(chunkIdWithIndex);
        }
    }

    if (chunk->IsErasure()) {
        auto repairIt = chunk->GetRepairQueueIterator();
        if (repairIt != TChunkRepairQueueIterator()) {
            RepairQueue.erase(repairIt);
            chunk->SetRepairQueueIterator(TChunkRepairQueueIterator());
        }
    }
}

void TChunkReplicator::CancelChunkJobs(TChunk* chunk)
{
    auto it = JobListMap.find(chunk);
    if (it == JobListMap.end())
        return;

    auto jobList = it->second;
    FOREACH (auto job, jobList->Jobs()) {
        UnregisterJob(job, EJobUnregisterFlags::UnregisterFromNode);
    }
    JobListMap.erase(it);
}

bool TChunkReplicator::IsReplicaDecommissioned(TNodePtrWithIndex replica)
{
    auto* node = replica.GetPtr();
    return node->GetDecommissioned();
}

bool TChunkReplicator::HasRunningJobs(TChunk* chunk)
{
    auto jobList = FindJobList(chunk);
    return jobList && !jobList->Jobs().empty();
}

bool TChunkReplicator::HasRunningJobs(TChunkPtrWithIndex replica)
{
    auto jobList = FindJobList(replica.GetPtr());
    if (!jobList) {
        return false;
    }

    FOREACH (const auto& job, jobList->Jobs()) {
        if (job->GetChunkIdWithIndex().Index == replica.GetIndex()) {
            return true;
        }
    }
    return false;
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
    objectManager->WeakRefObject(chunk);
}

void TChunkReplicator::ScheduleNodeRefresh(TNode* node)
{
    FOREACH (auto replica, node->StoredReplicas()) {
        ScheduleChunkRefresh(replica.GetPtr());
    }
}

void TChunkReplicator::OnRefresh()
{
    if (RefreshList.empty())
        return;

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

            objectManager->WeakUnrefObject(chunk);
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

    if (Config->DisableChunkReplicator) {
        if (!LastEnabled || LastEnabled.Get()) {
            LOG_INFO("Chunk replicator disabled by configuration settings");
            LastEnabled = false;
        }
        return false;
    }

    if (Config->SafeOnlineNodeCount) {
        int needOnline = *Config->SafeOnlineNodeCount;
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
    int lostChunkCount = chunkManager->LostVitalChunks().size();
    if (Config->SafeLostChunkFraction && chunkCount > 0) {
        double needFraction = *Config->SafeLostChunkFraction;
        double gotFraction = (double) lostChunkCount / chunkCount;
        if (gotFraction > needFraction) {
            if (!LastEnabled || LastEnabled.Get()) {
                LOG_INFO("Chunk replicator disabled: too many lost chunks, fraction needed <= %lf but got %lf",
                    needFraction,
                    gotFraction);
                LastEnabled = false;
            }
            return false;
        }
    }
    if (Config->SafeLostChunkCount && *Config->SafeLostChunkCount < lostChunkCount) {
        if (!LastEnabled || LastEnabled.Get()) {
            LOG_INFO("Chunk replicator disabled: too many lost chunks, needed <= %d but got %d",
                *Config->SafeLostChunkCount,
                lostChunkCount);
            LastEnabled = false;
        }
        return false;
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

int TChunkReplicator::GetPropertiesUpdateListSize() const
{
    return static_cast<int>(PropertiesUpdateList.size());
}

void TChunkReplicator::SchedulePropertiesUpdate(TChunkTree* chunkTree)
{
    switch (chunkTree->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            // Erasure chunks have no RF but still can update Vital.
            SchedulePropertiesUpdate(chunkTree->AsChunk());
            break;

        case EObjectType::ChunkList:
            SchedulePropertiesUpdate(chunkTree->AsChunkList());
            break;
        default:
            YUNREACHABLE();
    }
}

void TChunkReplicator::SchedulePropertiesUpdate(TChunkList* chunkList)
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
            TraverseChunkTree(CreateTraverserCallbacks(Bootstrap), this, Root);
        }

    private:
        TBootstrap* Bootstrap;
        TChunkReplicatorPtr Replicator;
        TChunkList* Root;

        virtual bool OnChunk(
            TChunk* chunk,
            i64 rowIndex,
            const TReadLimit& startLimit,
            const TReadLimit& endLimit) override
        {
            UNUSED(rowIndex);
            UNUSED(startLimit);
            UNUSED(endLimit);

            Replicator->SchedulePropertiesUpdate(chunk);
            return true;
        }

        virtual void OnError(const TError& error) override
        {
            LOG_ERROR(error, "Error traversing chunk tree for properties update");
        }

        virtual void OnFinish() override
        { }

    };

    New<TVisitor>(Bootstrap, this, chunkList)->Run();
}

void TChunkReplicator::SchedulePropertiesUpdate(TChunk* chunk)
{
    if (!IsObjectAlive(chunk) || chunk->GetPropertiesUpdateScheduled())
        return;

    PropertiesUpdateList.push_back(chunk);
    chunk->SetPropertiesUpdateScheduled(true);

    auto objectManager = Bootstrap->GetObjectManager();
    objectManager->WeakRefObject(chunk);
}

void TChunkReplicator::OnPropertiesUpdate()
{
    if (PropertiesUpdateList.empty() ||
        !Bootstrap->GetMetaStateFacade()->GetManager()->HasActiveQuorum())
    {
        PropertiesUpdateExecutor->ScheduleNext();
        return;
    }

    // Extract up to MaxChunksPerPropertiesUpdate objects and post a mutation.
    auto chunkManager = Bootstrap->GetChunkManager();
    auto objectManager = Bootstrap->GetObjectManager();
    TMetaReqUpdateChunkProperties request;

    PROFILE_TIMING ("/properties_update_time") {
        for (int i = 0; i < Config->MaxChunksPerPropertiesUpdate; ++i) {
            if (PropertiesUpdateList.empty())
                break;

            auto* chunk = PropertiesUpdateList.front();
            PropertiesUpdateList.pop_front();
            chunk->SetPropertiesUpdateScheduled(false);

            if (IsObjectAlive(chunk)) {
                auto newProperties = ComputeChunkProperties(chunk);
                auto oldProperties = chunk->GetChunkProperties();
                if (newProperties != oldProperties) {
                    auto* update = request.add_updates();
                    ToProto(update->mutable_chunk_id(), chunk->GetId());

                    if (newProperties.ReplicationFactor != oldProperties.ReplicationFactor) {
                        YCHECK(!chunk->IsErasure());
                        update->set_replication_factor(newProperties.ReplicationFactor);
                    }

                    if (newProperties.Vital != oldProperties.Vital) {
                        update->set_vital(newProperties.Vital);
                    }
                }
            }

            objectManager->WeakUnrefObject(chunk);
        }
    }

    if (request.updates_size() == 0) {
        PropertiesUpdateExecutor->ScheduleNext();
        return;
    }

    LOG_DEBUG("Starting properties update for %d chunks", request.updates_size());

    auto invoker = Bootstrap->GetMetaStateFacade()->GetEpochInvoker();
    chunkManager
        ->CreateUpdateChunkPropertiesMutation(request)
        ->OnSuccess(BIND(&TChunkReplicator::OnPropertiesUpdateCommitSucceeded, MakeWeak(this)).Via(invoker))
        ->OnError(BIND(&TChunkReplicator::OnPropertiesUpdateCommitFailed, MakeWeak(this)).Via(invoker))
        ->PostCommit();
}

void TChunkReplicator::OnPropertiesUpdateCommitSucceeded()
{
    LOG_DEBUG("Properties update commit succeeded");

    PropertiesUpdateExecutor->ScheduleOutOfBand();
    PropertiesUpdateExecutor->ScheduleNext();
}

void TChunkReplicator::OnPropertiesUpdateCommitFailed(const TError& error)
{
    LOG_WARNING(error, "Properties update commit failed");

    PropertiesUpdateExecutor->ScheduleNext();
}

TChunkProperties TChunkReplicator::ComputeChunkProperties(TChunk* chunk)
{
    bool parentsVisited = false;
    TChunkProperties properties;

    if (chunk->IsErasure()) {
        properties.ReplicationFactor = 1;
    }

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
                parentsVisited = true;

                if (!chunk->IsErasure()) {
                    properties.ReplicationFactor = std::max(
                        properties.ReplicationFactor,
                        owningNode->GetReplicationFactor());
                }

                properties.Vital |= owningNode->GetVital();
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

    return parentsVisited ? properties : chunk->GetChunkProperties();
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
    YCHECK(JobMap.insert(std::make_pair(job->GetJobId(), job)).second);
    YCHECK(job->GetNode()->Jobs().insert(job).second);

    auto chunkManager = Bootstrap->GetChunkManager();
    auto chunkId = job->GetChunkIdWithIndex().Id;
    auto* chunk = chunkManager->FindChunk(chunkId);
    if (chunk) {
        auto jobList = FindJobList(chunk);
        if (!jobList) {
            jobList = New<TJobList>();
            YCHECK(JobListMap.insert(std::make_pair(chunk, jobList)).second);
        }
        YCHECK(jobList->Jobs().insert(job).second);
    }

    LOG_INFO("Job registered (JobId: %s, JobType: %s, Address: %s)",
        ~ToString(job->GetJobId()),
        ~job->GetType().ToString(),
        ~job->GetNode()->GetAddress());
}

void TChunkReplicator::UnregisterJob(TJobPtr job, EJobUnregisterFlags flags)
{
    auto chunkManager = Bootstrap->GetChunkManager();
    auto chunkId = job->GetChunkIdWithIndex().Id;
    auto* chunk = chunkManager->FindChunk(chunkId);

    YCHECK(JobMap.erase(job->GetJobId()) == 1);

    if (flags & EJobUnregisterFlags::UnregisterFromNode) {
        YCHECK(job->GetNode()->Jobs().erase(job) == 1);
    }

    if (chunk) {
        if (flags & EJobUnregisterFlags::UnregisterFromChunk) {
            auto jobList = FindJobList(chunk);
            YCHECK(jobList);
            YCHECK(jobList->Jobs().erase(job) == 1);
            if (jobList->Jobs().empty()) {
                YCHECK(JobListMap.erase(chunk) == 1);
            }
        }

        if (flags & EJobUnregisterFlags::ScheduleChunkRefresh) {
            ScheduleChunkRefresh(chunk);
        }
    }

    LOG_INFO("Job unregistered (JobId: %s, Address: %s)",
        ~ToString(job->GetJobId()),
        ~job->GetNode()->GetAddress());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
