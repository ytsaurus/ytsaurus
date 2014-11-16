#include "stdafx.h"
#include "chunk_replicator.h"
#include "chunk_placement.h"
#include "job.h"
#include "chunk.h"
#include "chunk_list.h"
#include "chunk_owner_base.h"
#include "chunk_tree_traversing.h"
#include "private.h"

#include <core/misc/serialize.h>
#include <core/misc/string.h>
#include <core/misc/small_vector.h>
#include <core/misc/protobuf_helpers.h>

#include <core/erasure/codec.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/node_tracker_client/node_directory.h>
#include <ytlib/node_tracker_client/helpers.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <core/profiling/profiler.h>
#include <core/profiling/timing.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/config.h>
#include <server/cell_master/hydra_facade.h>

#include <server/chunk_server/chunk_manager.h>

#include <server/node_tracker_server/node_tracker.h>
#include <server/node_tracker_server/node.h>
#include <server/node_tracker_server/rack.h>
#include <server/node_tracker_server/node_directory_builder.h>

#include <server/cypress_server/node.h>

#include <array>

namespace NYT {
namespace NChunkServer {

using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NNodeTrackerServer;
using namespace NChunkServer::NProto;
using namespace NCellMaster;

using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;
static auto& Profiler = ChunkServerProfiler;

////////////////////////////////////////////////////////////////////////////////

TChunkReplicator::TChunkStatistics::TChunkStatistics()
{
    Zero(ReplicaCount);
    Zero(DecommissionedReplicaCount);
}

////////////////////////////////////////////////////////////////////////////////

TChunkReplicator::TChunkReplicator(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap,
    TChunkPlacementPtr chunkPlacement)
    : Config_(config)
    , Bootstrap_(bootstrap)
    , ChunkPlacement_(chunkPlacement)
    , ChunkRefreshDelay_(DurationToCpuDuration(config->ChunkRefreshDelay))
{
    YCHECK(config);
    YCHECK(bootstrap);
    YCHECK(chunkPlacement);

    auto nodeTracker = Bootstrap_->GetNodeTracker();
    for (const auto& pair : nodeTracker->Nodes()) {
        auto* node = pair.second;
        OnNodeRegistered(node);
    }

    auto chunkManager = Bootstrap_->GetChunkManager();
    for (const auto& pair : chunkManager->Chunks()) {
        auto* chunk = pair.second;
        ScheduleChunkRefresh(chunk);
        SchedulePropertiesUpdate(chunk);
    }
}

void TChunkReplicator::Start()
{
    RefreshExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkMaintenance),
        BIND(&TChunkReplicator::OnRefresh, MakeWeak(this)),
        Config_->ChunkRefreshPeriod);
    RefreshExecutor_->Start();

    PropertiesUpdateExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkMaintenance),
        BIND(&TChunkReplicator::OnPropertiesUpdate, MakeWeak(this)),
        Config_->ChunkPropertiesUpdatePeriod,
        EPeriodicExecutorMode::Manual);
    PropertiesUpdateExecutor_->Start();
}

void TChunkReplicator::Stop()
{
    auto nodeTracker = Bootstrap_->GetNodeTracker();
    for (const auto& pair : nodeTracker->Nodes()) {
        auto* node = pair.second;
        node->Jobs().clear();
    }

    RefreshExecutor_.Reset();
    PropertiesUpdateExecutor_.Reset();
}

void TChunkReplicator::TouchChunk(TChunk* chunk)
{
    auto repairIt = chunk->GetRepairQueueIterator();
    if (repairIt) {
        ChunkRepairQueue_.erase(*repairIt);
        auto newRepairIt = ChunkRepairQueue_.insert(ChunkRepairQueue_.begin(), chunk);
        chunk->SetRepairQueueIterator(newRepairIt);
    }
}

TJobPtr TChunkReplicator::FindJob(const TJobId& id)
{
    auto it = JobMap_.find(id);
    return it == JobMap_.end() ? nullptr : it->second;
}

TJobListPtr TChunkReplicator::FindJobList(TChunk* chunk)
{
    auto it = JobListMap_.find(chunk);
    return it == JobListMap_.end() ? nullptr : it->second;
}

EChunkStatus TChunkReplicator::ComputeChunkStatus(TChunk* chunk)
{
    auto statistics = ComputeChunkStatistics(chunk);
    return statistics.Status;
}

TChunkReplicator::TChunkStatistics TChunkReplicator::ComputeChunkStatistics(TChunk* chunk)
{
    switch (TypeFromId(chunk->GetId())) {
        case EObjectType::Chunk:
            return ComputeRegularChunkStatistics(chunk);
        case EObjectType::ErasureChunk:
            return ComputeErasureChunkStatistics(chunk);
        case EObjectType::JournalChunk:
            return ComputeJournalChunkStatistics(chunk);
        default:
            YUNREACHABLE();
    }
}

TChunkReplicator::TChunkStatistics TChunkReplicator::ComputeRegularChunkStatistics(TChunk* chunk)
{
    TChunkStatistics result;

    int replicationFactor = chunk->GetReplicationFactor();

    int replicaCount = 0;
    int decommissionedReplicaCount = 0;
    TNodePtrWithIndexList decommissionedReplicas;
    TRackSet usedRacks = 0;
    int usedRackCount = 0;

    for (auto replica : chunk->StoredReplicas()) {
        if (IsReplicaDecommissioned(replica)) {
            ++decommissionedReplicaCount;
            decommissionedReplicas.push_back(replica);
        } else {
            ++replicaCount;
        }
        const auto* rack = replica.GetPtr()->GetRack();
        auto rackMask = rack == nullptr ? NullRackMask : rack->GetIndexMask();
        if (!(usedRacks & rackMask)) {
            usedRacks |= rackMask;
            ++usedRackCount;
        }
    }

    result.ReplicaCount[GenericChunkReplicaIndex] = replicaCount;
    result.DecommissionedReplicaCount[GenericChunkReplicaIndex] = decommissionedReplicaCount;

    if (replicaCount + decommissionedReplicaCount == 0) {
        result.Status |= EChunkStatus::Lost;
    }
    
    if (replicaCount < replicationFactor && replicaCount + decommissionedReplicaCount > 0) {
        result.Status |= EChunkStatus::Underreplicated;
    }

    if (replicaCount == replicationFactor && decommissionedReplicaCount > 0) {
        result.Status |= EChunkStatus::Overreplicated;
        result.DecommissionedRemovalReplicas.append(decommissionedReplicas.begin(), decommissionedReplicas.end());
    }

    if (replicaCount > replicationFactor) {
        result.Status |= EChunkStatus::Overreplicated;
        result.BalancingRemovalIndexes.push_back(GenericChunkReplicaIndex);
    }

    if (usedRackCount == 1 && !(usedRacks & NullRackMask)) {
        // A regular chunk is considered placed unsafely if all of its replicas are placed in
        // one non-null rack.
        result.Status |= EChunkStatus::UnsafelyPlaced;
    }

    if (result.Status & EChunkStatus(EChunkStatus::Underreplicated | EChunkStatus::UnsafelyPlaced) &&
        replicaCount + decommissionedReplicaCount > 0)
    {
        result.ReplicationIndexes.push_back(GenericChunkReplicaIndex);
    }

    return result;
}

TChunkReplicator::TChunkStatistics TChunkReplicator::ComputeErasureChunkStatistics(TChunk* chunk)
{
    TChunkStatistics result;

    auto* codec = NErasure::GetCodec(chunk->GetErasureCodec());
    int totalPartCount = codec->GetTotalPartCount();
    int dataPartCount = codec->GetDataPartCount();
    int maxReplicasPerRack = codec->GetGuaranteedRepairablePartCount();
    std::array<TNodePtrWithIndexList, ChunkReplicaIndexBound> decommissionedReplicas{};
    std::array<ui8, MaxRackCount + 1> perRackReplicaCounters{};
    int unsafelyPlacedReplicaIndex = -1; // an arbitrary replica collocated with too may others within a single rack

    auto mark = TNode::GenerateVisitMark();

    for (auto replica : chunk->StoredReplicas()) {
        auto* node = replica.GetPtr();
        int index = replica.GetIndex();
        if (IsReplicaDecommissioned(replica) || node->GetVisitMark() == mark) {
            ++result.DecommissionedReplicaCount[index];
            decommissionedReplicas[index].push_back(replica);
        } else {
            ++result.ReplicaCount[index];
        }
        node->SetVisitMark(mark);
        const auto* rack = node->GetRack();
        if (rack) {
            int rackIndex = rack->GetIndex();
            if (++perRackReplicaCounters[rackIndex] > maxReplicasPerRack) {
                // An erasure chunk is considered placed unsafely if some non-null rack
                // contains more replicas than returned by ICodec::GetGuaranteedRepairablePartCount.
                unsafelyPlacedReplicaIndex = index;
            }
        }
    }

    NErasure::TPartIndexSet erasedIndexes;
    for (int index = 0; index < totalPartCount; ++index) {
        int replicaCount = result.ReplicaCount[index];
        int decommissionedReplicaCount = result.DecommissionedReplicaCount[index];
        
        if (replicaCount >= 1 && decommissionedReplicaCount > 0) {
            result.Status |= EChunkStatus::Overreplicated;
            const auto& replicas = decommissionedReplicas[index];
            result.DecommissionedRemovalReplicas.append(replicas.begin(), replicas.end());
        }

        if (replicaCount > 1 && decommissionedReplicaCount == 0) {
            result.Status |= EChunkStatus::Overreplicated;
            result.BalancingRemovalIndexes.push_back(index);
        }

        if (replicaCount == 0 && decommissionedReplicaCount > 0) {
            result.Status |= EChunkStatus::Underreplicated;
            result.ReplicationIndexes.push_back(index);
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

    if (unsafelyPlacedReplicaIndex != -1) {
        result.Status |= EChunkStatus::UnsafelyPlaced;
        if (result.ReplicationIndexes.empty()) {
            result.ReplicationIndexes.push_back(unsafelyPlacedReplicaIndex);
        }
    }

    return result;
}

TChunkReplicator::TChunkStatistics TChunkReplicator::ComputeJournalChunkStatistics(TChunk* chunk)
{
    TChunkStatistics result;

    int replicationFactor = chunk->GetReplicationFactor();
    int readQuorum = chunk->GetReadQuorum();

    int replicaCount = 0;
    int decommissionedReplicaCount = 0;
    int sealedReplicaCount = 0;
    int unsealedReplicaCount = 0;
    TNodePtrWithIndexList decommissionedReplicas;
    TRackSet usedRacks = 0;
    bool hasUnsafelyPlacedReplicas = false;

    for (auto replica : chunk->StoredReplicas()) {
        if (replica.GetIndex() == EJournalReplicaType::Sealed) {
            ++sealedReplicaCount;
        } else {
            ++unsealedReplicaCount;
        }
        if (IsReplicaDecommissioned(replica)) {
            ++decommissionedReplicaCount;
            decommissionedReplicas.push_back(replica);
        } else {
            ++replicaCount;
        }
        const auto* rack = replica.GetPtr()->GetRack();
        if (rack) {
            auto rackMask = rack->GetIndexMask();
            if (usedRacks & rackMask) {
                // A journal chunk is considered placed unsafely if some non-null rack
                // contains more than one of its replicas.
                hasUnsafelyPlacedReplicas = true;
            } else {
                usedRacks |= rackMask;
            }
        }
    }

    result.ReplicaCount[EJournalReplicaType::Generic] = replicaCount;
    result.DecommissionedReplicaCount[EJournalReplicaType::Generic] = decommissionedReplicaCount;

    if (replicaCount + decommissionedReplicaCount == 0) {
        result.Status |= EChunkStatus::Lost;
    }

    if (chunk->IsSealed()) {
        result.Status |= EChunkStatus::Sealed;

        if (replicaCount < replicationFactor && sealedReplicaCount > 0) {
            result.Status |= EChunkStatus::Underreplicated;
            result.ReplicationIndexes.push_back(GenericChunkReplicaIndex);
        }

        if (replicaCount == replicationFactor && decommissionedReplicaCount > 0 && unsealedReplicaCount == 0) {
            result.Status |= EChunkStatus::Overreplicated;
            result.DecommissionedRemovalReplicas.append(decommissionedReplicas.begin(), decommissionedReplicas.end());
        }

        if (replicaCount > replicationFactor && unsealedReplicaCount == 0) {
            result.Status |= EChunkStatus::Overreplicated;
            result.BalancingRemovalIndexes.push_back(GenericChunkReplicaIndex);
        }
    }
    
    if (replicaCount + decommissionedReplicaCount < readQuorum && sealedReplicaCount == 0) {
        result.Status |= EChunkStatus::QuorumMissing;
    }

    if (hasUnsafelyPlacedReplicas) {
        result.Status |= EChunkStatus::UnsafelyPlaced;
    }

    if (result.Status & EChunkStatus(EChunkStatus::Underreplicated | EChunkStatus::UnsafelyPlaced) &&
        sealedReplicaCount > 0)
    {
        result.ReplicationIndexes.push_back(GenericChunkReplicaIndex);
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
    node->ClearChunkRemovalQueue();
    node->ClearChunkReplicationQueues();
    node->ClearChunkSealQueue();
    ScheduleNodeRefresh(node);
}

void TChunkReplicator::OnNodeUnregistered(TNode* node)
{
    for (auto job : node->Jobs()) {
        UnregisterJob(
            job,
            EJobUnregisterFlags(EJobUnregisterFlags::UnregisterFromChunk | EJobUnregisterFlags::ScheduleChunkRefresh));
    }
    node->Jobs().clear();
}

void TChunkReplicator::OnChunkDestroyed(TChunk* chunk)
{
    ResetChunkStatus(chunk);
    ResetChunkJobs(chunk);

    {
        auto it = JobListMap_.find(chunk);
        if (it != JobListMap_.end()) {
            auto jobList = it->second;
            for (auto job : jobList->Jobs()) {
                UnregisterJob(job, EJobUnregisterFlags::UnregisterFromNode);
            }
            JobListMap_.erase(it);
        }
    }
}

void TChunkReplicator::ScheduleUnknownChunkRemoval(TNode* node, const TChunkIdWithIndex& chunkIdWithIndex)
{
    node->AddToChunkRemovalQueue(chunkIdWithIndex);
}

void TChunkReplicator::ScheduleChunkRemoval(TNode* node, TChunkPtrWithIndex chunkWithIndex)
{
    TChunkIdWithIndex chunkIdWithIndex(chunkWithIndex.GetPtr()->GetId(), chunkWithIndex.GetIndex());
    node->AddToChunkRemovalQueue(chunkIdWithIndex);
}

void TChunkReplicator::ProcessExistingJobs(
    TNode* node,
    const std::vector<TJobPtr>& currentJobs,
    std::vector<TJobPtr>* jobsToAbort,
    std::vector<TJobPtr>* jobsToRemove)
{
    const auto& address = node->GetAddress();

    auto chunkManager = Bootstrap_->GetChunkManager();
    for (const auto& job : currentJobs) {
        if (job->GetType() == EJobType::Foreign)
            continue;

        const auto& jobId = job->GetJobId();
        switch (job->GetState()) {
            case EJobState::Running:
                if (TInstant::Now() - job->GetStartTime() > Config_->JobTimeout) {
                    jobsToAbort->push_back(job);
                    LOG_WARNING("Job timed out (JobId: %v, Address: %v, Duration: %v)",
                        jobId,
                        address,
                        TInstant::Now() - job->GetStartTime());
                } else {
                    LOG_INFO("Job is running (JobId: %v, Address: %v)",
                        jobId,
                        address);
                }
                break;

            case EJobState::Completed:
            case EJobState::Failed:
            case EJobState::Aborted: {
                jobsToRemove->push_back(job);
                switch (job->GetState()) {
                    case EJobState::Completed:
                        LOG_INFO("Job completed (JobId: %v, Address: %v)",
                            jobId,
                            address);
                        break;

                    case EJobState::Failed:
                        LOG_WARNING(job->Error(), "Job failed (JobId: %v, Address: %v)",
                            jobId,
                            address);
                        break;

                    case EJobState::Aborted:
                        LOG_WARNING(job->Error(), "Job aborted (JobId: %v, Address: %v)",
                            jobId,
                            address);
                        break;

                    default:
                        YUNREACHABLE();
                }
                UnregisterJob(job);
                break;
            }

            case EJobState::Waiting:
                LOG_INFO("Job is waiting (JobId: %v, Address: %v)",
                    jobId,
                    address);
                break;

            default:
                YUNREACHABLE();
        }
    }

    // Check for missing jobs
    yhash_set<TJobPtr> currentJobSet(currentJobs.begin(), currentJobs.end());
    std::vector<TJobPtr> missingJobs;
    for (const auto& job : node->Jobs()) {
        if (currentJobSet.find(job) == currentJobSet.end()) {
            missingJobs.push_back(job);
            LOG_WARNING("Job is missing (JobId: %v, Address: %v)",
                job->GetJobId(),
                address);
        }
    }

    for (const auto& job : missingJobs) {
        UnregisterJob(job);
    }
}

bool TChunkReplicator::CreateReplicationJob(
    TNode* sourceNode,
    TChunkPtrWithIndex chunkWithIndex,
    TJobPtr* job)
{
    auto* chunk = chunkWithIndex.GetPtr();
    int index = chunkWithIndex.GetIndex();

    if (!IsObjectAlive(chunk)) {
        return true;
    }

    if (chunk->GetRefreshScheduled()) {
        return true;
    }

    if (HasRunningJobs(chunkWithIndex)) {
        return true;
    }

    int replicationFactor = chunk->GetReplicationFactor();
    auto statistics = ComputeChunkStatistics(chunk);
    int replicaCount = statistics.ReplicaCount[index];
    int decommissionedReplicaCount = statistics.DecommissionedReplicaCount[index];

    if (replicaCount + decommissionedReplicaCount == 0) {
        return true;
    }

    int replicasNeeded;
    if (statistics.Status & EChunkStatus::Underreplicated) {
        replicasNeeded = replicationFactor - replicaCount;
    } else if (statistics.Status & EChunkStatus::UnsafelyPlaced) {
        replicasNeeded = 1;
    } else {
        return true;
    }

    // TODO(babenko): journal replication currently does not support fan-out > 1
    if (chunk->IsJournal()) {
        replicasNeeded = 1;
    }

    auto targets = ChunkPlacement_->AllocateWriteTargets(
        chunk,
        replicasNeeded,
        EWriteSessionType::Replication);
    if (targets.empty()) {
        return false;
    }

    TNodeResources resourceUsage;
    resourceUsage.set_replication_slots(1);

    *job = TJob::CreateReplicate(
        TChunkIdWithIndex(chunk->GetId(), index),
        sourceNode,
        targets,
        resourceUsage);

    LOG_INFO("Replication job scheduled (JobId: %v, Address: %v, ChunkId: %v, TargetAddresses: [%v])",
        (*job)->GetJobId(),
        sourceNode->GetAddress(),
        chunkWithIndex,
        JoinToString(targets, TNodePtrAddressFormatter()));

    return targets.size() == replicasNeeded;
}

bool TChunkReplicator::CreateBalancingJob(
    TNode* sourceNode,
    TChunkPtrWithIndex chunkWithIndex,
    double maxFillFactor,
    TJobPtr* job)
{
    TChunkIdWithIndex chunkIdWithIndex(chunkWithIndex.GetPtr()->GetId(), chunkWithIndex.GetIndex());
    auto* chunk = chunkWithIndex.GetPtr();

    if (chunk->GetRefreshScheduled()) {
        return true;
    }

    auto* target = ChunkPlacement_->AllocateBalancingTarget(chunkWithIndex, maxFillFactor);
    if (!target) {
        return false;
    }

    TNodeResources resourceUsage;
    resourceUsage.set_replication_slots(1);

    *job = TJob::CreateReplicate(
        chunkIdWithIndex,
        sourceNode,
        TNodeList(1, target),
        resourceUsage);

    LOG_INFO("Balancing job scheduled (JobId: %v, Address: %v, ChunkId: %v, TargetAddress: %v)",
        (*job)->GetJobId(),
        sourceNode->GetAddress(),
        chunkIdWithIndex,
        target->GetAddress());

    return true;
}

bool TChunkReplicator::CreateRemovalJob(
    TNode* node,
    const TChunkIdWithIndex& chunkIdWithIndex,
    TJobPtr* job)
{
    auto chunkManager = Bootstrap_->GetChunkManager();
    auto* chunk = chunkManager->FindChunk(chunkIdWithIndex.Id);
    // NB: Allow more than one job for dead chunks.
    if (chunk) {
        if (chunk->GetRefreshScheduled()) {
            return true;
        }
        if (HasRunningJobs(TChunkPtrWithIndex(chunk, chunkIdWithIndex.Index))) {
            return true;
        }
    }

    TNodeResources resourceUsage;
    resourceUsage.set_removal_slots(1);

    *job = TJob::CreateRemove(
        chunkIdWithIndex,
        node,
        resourceUsage);

    LOG_INFO("Removal job scheduled (JobId: %v, Address: %v, ChunkId: %v)",
        (*job)->GetJobId(),
        node->GetAddress(),
        chunkIdWithIndex);

    return true;
}

bool TChunkReplicator::CreateRepairJob(
    TNode* node,
    TChunk* chunk,
    TJobPtr* job)
{
    YCHECK(chunk->IsErasure());

    if (!IsObjectAlive(chunk)) {
        return true;
    }

    if (chunk->GetRefreshScheduled()) {
        return true;
    }

    if (HasRunningJobs(chunk)) {
        return true;
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
        return true;
    }

    auto targets = ChunkPlacement_->AllocateWriteTargets(
        chunk,
        erasedIndexCount,
        EWriteSessionType::Repair);
    if (targets.empty()) {
        return false;
    }

    TNodeResources resourceUsage;
    resourceUsage.set_repair_slots(1);
    resourceUsage.set_memory(Config_->RepairJobMemoryUsage);

    *job = TJob::CreateRepair(
        chunk->GetId(),
        node,
        targets,
        erasedIndexes,
        resourceUsage);

    LOG_INFO("Repair job scheduled (JobId: %v, Address: %v, ChunkId: %v, TargetAddresses: [%v], ErasedIndexes: [%v])",
        (*job)->GetJobId(),
        node->GetAddress(),
        chunk->GetId(),
        JoinToString(targets, TNodePtrAddressFormatter()),
        JoinToString(erasedIndexes));

    return true;
}


bool TChunkReplicator::CreateSealJob(
    TNode* node,
    TChunk* chunk,
    TJobPtr* job)
{
    YCHECK(chunk->IsJournal());
    YCHECK(chunk->IsSealed());

    if (!IsObjectAlive(chunk)) {
        return true;
    }

    // NB: Seal jobs can be started even if chunk refresh is scheduled.

    if (chunk->StoredReplicas().size() < chunk->GetReadQuorum()) {
        return true;
    }

    TNodeResources resourceUsage;
    resourceUsage.set_seal_slots(1);

    *job = TJob::CreateSeal(
        chunk->GetId(),
        node,
        resourceUsage);

    LOG_INFO("Seal job scheduled (JobId: %v, Address: %v, ChunkId: %v)",
        (*job)->GetJobId(),
        node->GetAddress(),
        chunk->GetId());

    return true;
}

void TChunkReplicator::ScheduleNewJobs(
    TNode* node,
    std::vector<TJobPtr>* jobsToStart,
    std::vector<TJobPtr>* jobsToAbort)
{
    auto chunkManager = Bootstrap_->GetChunkManager();

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

        // Adjust data size of erasure chunk replicas.
        auto codecId = chunk->GetErasureCodec();
        if (codecId != NErasure::ECodec::None) {
            auto* codec = NErasure::GetCodec(codecId);
            size /= codec->GetTotalPartCount();
        }

        switch (type) {
            case EJobType::ReplicateChunk:
                runningReplicationSize += size;
                break;
            case EJobType::RepairChunk:
                runningRepairSize += size;
                break;
            default:
                break;
        }
    };

    // Compute current data sizes for running replication and repair jobs.
    for (auto job : node->Jobs()) {
        increaseRunningSizes(job);
    }

    auto registerJob = [&] (TJobPtr job) {
        if (job) {
            jobsToStart->push_back(job);
            RegisterJob(job);
            resourceUsage += job->ResourceUsage();
            increaseRunningSizes(job);
        }
    };

    // Schedule replication jobs.
    for (auto& queue : node->ChunkReplicationQueues()) {
        auto it = queue.begin();
        while (it != queue.end()) {
            if (resourceUsage.replication_slots() >= resourceLimits.replication_slots())
                break;
            if (runningReplicationSize > Config_->MaxReplicationJobsSize)
                break;

            auto jt = it++;
            auto chunkWithIndex = *jt;

            TJobPtr job;
            if (CreateReplicationJob(node, chunkWithIndex, &job)) {
                queue.erase(jt);
            }
            registerJob(job);
        }
    }

    // Schedule repair jobs.
    {
        auto it = ChunkRepairQueue_.begin();
        while (it != ChunkRepairQueue_.end()) {
            if (resourceUsage.repair_slots() >= resourceLimits.repair_slots())
                break;
            if (runningRepairSize > Config_->MaxRepairJobsSize)
                break;

            auto jt = it++;
            auto* chunk = *jt;

            TJobPtr job;
            if (CreateRepairJob(node, chunk, &job)) {
                chunk->SetRepairQueueIterator(Null);
                ChunkRepairQueue_.erase(jt);
            }
            registerJob(job);
        }
    }

    // Schedule removal jobs.
    {
        auto& queue = node->ChunkRemovalQueue();
        auto it = queue.begin();
        while (it != queue.end()) {
            if (resourceUsage.removal_slots() >= resourceLimits.removal_slots())
                break;

            auto jt = it++;
            const auto& chunkId = *jt;

            TJobPtr job;
            if (CreateRemovalJob(node, chunkId, &job)) {
                queue.erase(jt);
            }
            registerJob(job);
        }
    }

    // Schedule seal jobs.
    {
        auto& queue = node->ChunkSealQueue();
        auto it = queue.begin();
        while (it != queue.end()) {
            if (resourceUsage.seal_slots() >= resourceLimits.seal_slots())
                break;

            auto jt = it++;
            auto* chunk = *jt;

            TJobPtr job;
            if (CreateSealJob(node, chunk, &job)) {
                queue.erase(jt);
            }
            registerJob(job);
        }
    }

    // Schedule balancing jobs.
    double sourceFillFactor = ChunkPlacement_->GetFillFactor(node);
    double targetFillFactor = sourceFillFactor - Config_->MinBalancingFillFactorDiff;
    if (resourceUsage.replication_slots() < resourceLimits.replication_slots() &&
        sourceFillFactor > Config_->MinBalancingFillFactor &&
        ChunkPlacement_->HasBalancingTargets(targetFillFactor))
    {
        int maxJobs = std::max(0, resourceLimits.replication_slots() - resourceUsage.replication_slots());
        auto chunksToBalance = ChunkPlacement_->GetBalancingChunks(node, maxJobs);
        for (auto chunkWithIndex : chunksToBalance) {
            if (resourceUsage.replication_slots() >= resourceLimits.replication_slots())
                break;
            if (runningReplicationSize > Config_->MaxReplicationJobsSize)
                break;

            TJobPtr job;
            CreateBalancingJob(node, chunkWithIndex, targetFillFactor, &job);
            registerJob(job);
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

    if (statistics.Status & EChunkStatus::QuorumMissing) {
        YCHECK(QuorumMissingChunks_.insert(chunk).second);
    }

    if (statistics.Status & EChunkStatus::UnsafelyPlaced) {
        YCHECK(UnsafelyPlacedChunks_.insert(chunk).second);
    }

    if (!HasRunningJobs(chunk)) {
        ResetChunkJobs(chunk);

        if (statistics.Status & EChunkStatus::Overreplicated) {
            for (auto nodeWithIndex : statistics.DecommissionedRemovalReplicas) {
                int index = nodeWithIndex.GetIndex();
                TChunkIdWithIndex chunkIdWithIndex(chunk->GetId(), index);
                nodeWithIndex.GetPtr()->AddToChunkRemovalQueue(chunkIdWithIndex);
            }

            for (int index : statistics.BalancingRemovalIndexes) {
                TChunkPtrWithIndex chunkWithIndex(chunk, index);
                TChunkIdWithIndex chunkIdWithIndex(chunk->GetId(), index);
                auto* target = ChunkPlacement_->GetRemovalTarget(chunkWithIndex);
                if (target) {
                    target->AddToChunkRemovalQueue(chunkIdWithIndex);
                }
            }
        }

        if (statistics.Status & (EChunkStatus::Underreplicated | EChunkStatus::UnsafelyPlaced)) {
            for (int index : statistics.ReplicationIndexes) {
                TChunkPtrWithIndex chunkWithIndex(chunk, index);
                TChunkIdWithIndex chunkIdWithIndex(chunk->GetId(), index);

                // Cap replica count minus one against the range [0, ReplicationPriorityCount - 1].
                int replicaCount = statistics.ReplicaCount[index];
                int priority = std::max(std::min(replicaCount - 1, ReplicationPriorityCount - 1), 0);

                for (auto replica : chunk->StoredReplicas()) {
                    if (chunk->IsRegular() ||
                        chunk->IsErasure() && replica.GetIndex() == index ||
                        chunk->IsJournal() && replica.GetIndex() == EJournalReplicaType::Sealed)
                    {
                        replica.GetPtr()->AddToChunkReplicationQueue(chunkWithIndex, priority);
                    }
                }
            }
        }

        if (statistics.Status & EChunkStatus::Sealed) {
            YASSERT(chunk->IsJournal());
            for (auto replica : chunk->StoredReplicas()) {
                if (replica.GetIndex() == EJournalReplicaType::Unsealed) {
                    replica.GetPtr()->AddToChunkSealQueue(chunk);
                }
            }
        }

        if ((statistics.Status & EChunkStatus(EChunkStatus::DataMissing | EChunkStatus::ParityMissing)) &&
            !(statistics.Status & EChunkStatus::Lost))
        {
            AddToChunkRepairQueue(chunk);
        }
    }
}

void TChunkReplicator::ResetChunkStatus(TChunk* chunk)
{
    LostChunks_.erase(chunk);
    LostVitalChunks_.erase(chunk);
    UnderreplicatedChunks_.erase(chunk);
    OverreplicatedChunks_.erase(chunk);
    UnsafelyPlacedChunks_.erase(chunk);

    if (chunk->IsErasure()) {
        DataMissingChunks_.erase(chunk);
        ParityMissingChunks_.erase(chunk);
    }

    if (chunk->IsJournal()) {
        QuorumMissingChunks_.erase(chunk);
    }
}

void TChunkReplicator::ResetChunkJobs(TChunk* chunk)
{
    for (auto nodeWithIndex : chunk->StoredReplicas()) {
        auto* node = nodeWithIndex.GetPtr();
        TChunkPtrWithIndex chunkWithIndex(chunk, nodeWithIndex.GetIndex());
        TChunkIdWithIndex chunkIdWithIndex(chunk->GetId(), nodeWithIndex.GetIndex());
        node->RemoveFromChunkRemovalQueue(chunkIdWithIndex);
        node->RemoveFromChunkReplicationQueues(chunkWithIndex);
        node->RemoveFromChunkSealQueue(chunk);
    }

    if (chunk->IsErasure()) {
        RemoveFromChunkRepairQueue(chunk);
    }
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
    
    auto* chunk = replica.GetPtr();
    if (chunk->IsJournal()) {
        if (!jobList->Jobs().empty()) {
            return true;
        }
    } else {
        for (const auto& job : jobList->Jobs()) {
            if (job->GetChunkIdWithIndex().Index == replica.GetIndex()) {
                return true;
            }
        }
    }

    return false;
}

void TChunkReplicator::ScheduleChunkRefresh(const TChunkId& chunkId)
{
    auto chunkManager = Bootstrap_->GetChunkManager();
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
    entry.When = GetCpuInstant() + ChunkRefreshDelay_;
    RefreshList_.push_back(entry);
    chunk->SetRefreshScheduled(true);

    auto objectManager = Bootstrap_->GetObjectManager();
    objectManager->WeakRefObject(chunk);
}

void TChunkReplicator::ScheduleNodeRefresh(TNode* node)
{
    for (auto replica : node->StoredReplicas()) {
        ScheduleChunkRefresh(replica.GetPtr());
    }
}

void TChunkReplicator::OnRefresh()
{
    if (RefreshList_.empty())
        return;

    auto objectManager = Bootstrap_->GetObjectManager();

    int count = 0;
    PROFILE_TIMING ("/incremental_refresh_time") {
        auto chunkManager = Bootstrap_->GetChunkManager();
        auto now = GetCpuInstant();
        for (int i = 0; i < Config_->MaxChunksPerRefresh; ++i) {
            if (RefreshList_.empty())
                break;

            const auto& entry = RefreshList_.front();
            if (entry.When > now)
                break;

            auto* chunk = entry.Chunk;
            RefreshList_.pop_front();
            chunk->SetRefreshScheduled(false);
            ++count;

            if (IsObjectAlive(chunk)) {
                RefreshChunk(chunk);
            }

            objectManager->WeakUnrefObject(chunk);
        }
    }

    LOG_DEBUG("Incremental chunk refresh completed, %v chunks processed",
        count);
}

bool TChunkReplicator::IsEnabled()
{
    // This method also logs state changes.

    auto chunkManager = Bootstrap_->GetChunkManager();
    auto nodeTracker = Bootstrap_->GetNodeTracker();

    if (Config_->DisableChunkReplicator) {
        if (!LastEnabled_ || LastEnabled_.Get()) {
            LOG_INFO("Chunk replicator disabled by configuration settings");
            LastEnabled_ = false;
        }
        return false;
    }

    if (Config_->SafeOnlineNodeCount) {
        int needOnline = *Config_->SafeOnlineNodeCount;
        int gotOnline = nodeTracker->GetOnlineNodeCount();
        if (gotOnline < needOnline) {
            if (!LastEnabled_ || LastEnabled_.Get()) {
                LOG_INFO("Chunk replicator disabled: too few online nodes, needed >= %v but got %v",
                    needOnline,
                    gotOnline);
                LastEnabled_ = false;
            }
            return false;
        }
    }

    int chunkCount = chunkManager->Chunks().GetSize();
    int lostChunkCount = chunkManager->LostChunks().size();
    if (Config_->SafeLostChunkFraction && chunkCount > 0) {
        double needFraction = *Config_->SafeLostChunkFraction;
        double gotFraction = (double) lostChunkCount / chunkCount;
        if (gotFraction > needFraction) {
            if (!LastEnabled_ || LastEnabled_.Get()) {
                LOG_INFO("Chunk replicator disabled: too many lost chunks, needed <= %lf but got %lf",
                    needFraction,
                    gotFraction);
                LastEnabled_ = false;
            }
            return false;
        }
    }

    if (!LastEnabled_ || !*LastEnabled_) {
        LOG_INFO("Chunk replicator enabled");
        LastEnabled_ = true;
    }

    return true;
}

int TChunkReplicator::GetRefreshListSize() const
{
    return static_cast<int>(RefreshList_.size());
}

int TChunkReplicator::GetPropertiesUpdateListSize() const
{
    return static_cast<int>(PropertiesUpdateList_.size());
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
            TraverseChunkTree(CreatePreemptableChunkTraverserCallbacks(Bootstrap), this, Root);
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

    New<TVisitor>(Bootstrap_, this, chunkList)->Run();
}

void TChunkReplicator::SchedulePropertiesUpdate(TChunk* chunk)
{
    if (!IsObjectAlive(chunk) || chunk->GetPropertiesUpdateScheduled())
        return;

    PropertiesUpdateList_.push_back(chunk);
    chunk->SetPropertiesUpdateScheduled(true);

    auto objectManager = Bootstrap_->GetObjectManager();
    objectManager->WeakRefObject(chunk);
}

void TChunkReplicator::OnPropertiesUpdate()
{
    if (PropertiesUpdateList_.empty() ||
        !Bootstrap_->GetHydraFacade()->GetHydraManager()->IsActiveLeader())
    {
        PropertiesUpdateExecutor_->ScheduleNext();
        return;
    }

    // Extract up to MaxChunksPerPropertiesUpdate objects and post a mutation.
    auto chunkManager = Bootstrap_->GetChunkManager();
    auto objectManager = Bootstrap_->GetObjectManager();
    TReqUpdateChunkProperties request;

    PROFILE_TIMING ("/properties_update_time") {
        for (int i = 0; i < Config_->MaxChunksPerPropertiesUpdate; ++i) {
            if (PropertiesUpdateList_.empty())
                break;

            auto* chunk = PropertiesUpdateList_.front();
            PropertiesUpdateList_.pop_front();
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
        PropertiesUpdateExecutor_->ScheduleNext();
        return;
    }

    LOG_DEBUG("Starting properties update for %v chunks", request.updates_size());

    auto this_ = MakeStrong(this);
    auto invoker = Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker();
    chunkManager
        ->CreateUpdateChunkPropertiesMutation(request)
        ->Commit()
        .Subscribe(BIND([this, this_] (const TErrorOr<TMutationResponse>& error) {
            if (error.IsOK()) {
                PropertiesUpdateExecutor_->ScheduleOutOfBand();
            }
            PropertiesUpdateExecutor_->ScheduleNext();
        }).Via(invoker));
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
    SmallVector<TChunkList*, 64> queue;
    size_t frontIndex = 0;

    auto enqueue = [&] (TChunkList* chunkList) {
        if (chunkList->GetVisitMark() != mark) {
            chunkList->SetVisitMark(mark);
            queue.push_back(chunkList);
        }
    };

    // Put seeds into the queue.
    for (auto* parent : chunk->Parents()) {
        auto* adjustedParent = FollowParentLinks(parent);
        if (adjustedParent) {
            enqueue(adjustedParent);
        }
    }

    // The main BFS loop.
    while (frontIndex < queue.size()) {
        auto* chunkList = queue[frontIndex++];

        // Examine owners, if any.
        for (const auto* owningNode : chunkList->OwningNodes()) {
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
        for (auto* parent : chunkList->Parents()) {
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
    YCHECK(JobMap_.insert(std::make_pair(job->GetJobId(), job)).second);
    YCHECK(job->GetNode()->Jobs().insert(job).second);

    auto chunkManager = Bootstrap_->GetChunkManager();
    auto chunkId = job->GetChunkIdWithIndex().Id;
    auto* chunk = chunkManager->FindChunk(chunkId);
    if (chunk) {
        auto jobList = FindJobList(chunk);
        if (!jobList) {
            jobList = New<TJobList>();
            YCHECK(JobListMap_.insert(std::make_pair(chunk, jobList)).second);
        }
        YCHECK(jobList->Jobs().insert(job).second);
    }

    LOG_INFO("Job registered (JobId: %v, JobType: %v, Address: %v)",
        job->GetJobId(),
        job->GetType(),
        job->GetNode()->GetAddress());
}

void TChunkReplicator::UnregisterJob(TJobPtr job, EJobUnregisterFlags flags)
{
    auto chunkManager = Bootstrap_->GetChunkManager();
    auto chunkId = job->GetChunkIdWithIndex().Id;
    auto* chunk = chunkManager->FindChunk(chunkId);

    YCHECK(JobMap_.erase(job->GetJobId()) == 1);

    if (flags & EJobUnregisterFlags::UnregisterFromNode) {
        YCHECK(job->GetNode()->Jobs().erase(job) == 1);
    }

    if (chunk) {
        if (flags & EJobUnregisterFlags::UnregisterFromChunk) {
            auto jobList = FindJobList(chunk);
            YCHECK(jobList);
            YCHECK(jobList->Jobs().erase(job) == 1);
            if (jobList->Jobs().empty()) {
                YCHECK(JobListMap_.erase(chunk) == 1);
            }
        }

        if (flags & EJobUnregisterFlags::ScheduleChunkRefresh) {
            ScheduleChunkRefresh(chunk);
        }
    }

    LOG_INFO("Job unregistered (JobId: %v, Address: %v)",
        job->GetJobId(),
        job->GetNode()->GetAddress());
}

void TChunkReplicator::AddToChunkRepairQueue(TChunk* chunk)
{
    YASSERT(!chunk->GetRepairQueueIterator());
    auto it = ChunkRepairQueue_.insert(ChunkRepairQueue_.end(), chunk);
    chunk->SetRepairQueueIterator(it);
}

void TChunkReplicator::RemoveFromChunkRepairQueue(TChunk* chunk)
{
    auto it = chunk->GetRepairQueueIterator();
    if (it) {
        ChunkRepairQueue_.erase(*it);
        chunk->SetRepairQueueIterator(Null);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
