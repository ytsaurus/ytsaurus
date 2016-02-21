#include "chunk_replicator.h"
#include "private.h"
#include "chunk.h"
#include "chunk_list.h"
#include "chunk_owner_base.h"
#include "chunk_placement.h"
#include "chunk_tree_traversing.h"
#include "job.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/config.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/world_initializer.h>
#include <yt/server/cell_master/multicell_manager.h>
#include <yt/server/cell_master/multicell_manager.pb.h>

#include <yt/server/chunk_server/chunk_manager.h>

#include <yt/server/cypress_server/node.h>
#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/node_tracker_server/node.h>
#include <yt/server/node_tracker_server/node_directory_builder.h>
#include <yt/server/node_tracker_server/node_tracker.h>
#include <yt/server/node_tracker_server/rack.h>

#include <yt/server/object_server/object.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/ytlib/node_tracker_client/helpers.h>
#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/erasure/codec.h>

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/serialize.h>
#include <yt/core/misc/small_vector.h>
#include <yt/core/misc/string.h>

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/timing.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/ytree/ypath_proxy.h>

#include <array>

namespace NYT {
namespace NChunkServer {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NHydra;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NNodeTrackerServer;
using namespace NObjectServer;
using namespace NChunkServer::NProto;
using namespace NCellMaster;

using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;
static const auto& Profiler = ChunkServerProfiler;
static const auto EnabledCheckPeriod = TDuration::Seconds(3);

////////////////////////////////////////////////////////////////////////////////

TChunkReplicator::TChunkStatistics::TChunkStatistics()
    : Status(EChunkStatus::None)
    , ReplicaCount{}
    , DecommissionedReplicaCount{}
{ }

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
    YCHECK(Config_);
    YCHECK(Bootstrap_);
    YCHECK(ChunkPlacement_);
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
        Config_->ChunkPropertiesUpdatePeriod);
    PropertiesUpdateExecutor_->Start();

    EnabledCheckExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkMaintenance),
        BIND(&TChunkReplicator::OnCheckEnabled, MakeWeak(this)),
        Config_->ReplicatorEnabledCheckPeriod);
    EnabledCheckExecutor_->Start();
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
    EnabledCheckExecutor_.Reset();
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

    int replicationFactor = chunk->ComputeReplicationFactor();

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

    if (replicationFactor > 1 && usedRackCount == 1 && usedRacks != NullRackMask) {
        // A regular chunk is considered placed unsafely if all of its replicas are placed in
        // one non-null rack. Also, for RF=1 rack awareness is effectively off.
        result.Status |= EChunkStatus::UnsafelyPlaced;
    }

    if (Any(result.Status & (EChunkStatus::Underreplicated | EChunkStatus::UnsafelyPlaced)) &&
        None(result.Status & EChunkStatus::Overreplicated) &&
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
                // A erasure chunk is considered placed unsafely if some non-null rack
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
        if (None(result.Status & EChunkStatus::Overreplicated) && result.ReplicationIndexes.empty()) {
            result.ReplicationIndexes.push_back(unsafelyPlacedReplicaIndex);
        }
    }

    return result;
}

TChunkReplicator::TChunkStatistics TChunkReplicator::ComputeJournalChunkStatistics(TChunk* chunk)
{
    TChunkStatistics result;

    int replicationFactor = chunk->ComputeReplicationFactor();
    int readQuorum = chunk->GetReadQuorum();

    int replicaCount = 0;
    int decommissionedReplicaCount = 0;
    int sealedReplicaCount = 0;
    int unsealedReplicaCount = 0;
    TNodePtrWithIndexList decommissionedReplicas;
    TRackSet usedRacks = 0;
    bool hasUnsafelyPlacedReplicas = false;

    for (auto replica : chunk->StoredReplicas()) {
        if (replica.GetIndex() == SealedChunkReplicaIndex) {
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

    result.ReplicaCount[GenericChunkReplicaIndex] = replicaCount;
    result.DecommissionedReplicaCount[GenericChunkReplicaIndex] = decommissionedReplicaCount;

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

    if (Any(result.Status & (EChunkStatus::Underreplicated | EChunkStatus::UnsafelyPlaced)) &&
        None(result.Status & EChunkStatus::Overreplicated) &&
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

    ScheduleNewJobs(
        node,
        jobsToStart,
        jobsToAbort);
}

void TChunkReplicator::OnNodeRegistered(TNode* /*node*/)
{ }

void TChunkReplicator::OnNodeUnregistered(TNode* node)
{
    for (const auto& job : node->Jobs()) {
        UnregisterJob(
            job,
            EJobUnregisterFlags(EJobUnregisterFlags::UnregisterFromChunk | EJobUnregisterFlags::ScheduleChunkRefresh));
    }
    node->Reset();
}

void TChunkReplicator::OnNodeDisposed(TNode* node)
{
    YCHECK(node->Jobs().empty());
    YCHECK(node->ChunkSealQueue().empty());
    YCHECK(node->ChunkRemovalQueue().empty());
    for (const auto& queue : node->ChunkReplicationQueues()) {
        YCHECK(queue.empty());
    }
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

void TChunkReplicator::OnReplicaRemoved(
    TNode* node,
    TChunkPtrWithIndex chunkWithIndex,
    ERemoveReplicaReason reason)
{
    RemoveReplicaFromQueues(
        chunkWithIndex.GetPtr(),
        TNodePtrWithIndex(node, chunkWithIndex.GetIndex()),
        reason != ERemoveReplicaReason::ChunkIsDead);
}

void TChunkReplicator::ScheduleUnknownReplicaRemoval(
    TNode* node,
    const TChunkIdWithIndex& chunkIdWithIndex)
{
    node->AddToChunkRemovalQueue(chunkIdWithIndex);
}

void TChunkReplicator::ScheduleReplicaRemoval(
    TNode* node,
    TChunkPtrWithIndex chunkWithIndex)
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
    const auto& address = node->GetDefaultAddress();

    auto chunkManager = Bootstrap_->GetChunkManager();
    for (const auto& job : currentJobs) {
        const auto& jobId = job->GetJobId();
        YCHECK(CellTagFromId(jobId) == Bootstrap_->GetCellTag());
        YCHECK(TypeFromId(jobId) == EObjectType::MasterJob);
        switch (job->GetState()) {
            case EJobState::Running:
            case EJobState::Waiting: {
                if (TInstant::Now() - job->GetStartTime() > Config_->JobTimeout) {
                    jobsToAbort->push_back(job);
                    LOG_WARNING("Job timed out (JobId: %v, Address: %v, Duration: %v)",
                        jobId,
                        address,
                        TInstant::Now() - job->GetStartTime());
                    break;
                }

                switch (job->GetState()) {
                    case EJobState::Running:
                        LOG_DEBUG("Job is running (JobId: %v, Address: %v)",
                            jobId,
                            address);
                        break;

                    case EJobState::Waiting:
                        LOG_DEBUG("Job is waiting (JobId: %v, Address: %v)",
                            jobId,
                            address);
                        break;

                    default:
                        YUNREACHABLE();
                }
                break;
            }

            case EJobState::Completed:
            case EJobState::Failed:
            case EJobState::Aborted: {
                jobsToRemove->push_back(job);
                switch (job->GetState()) {
                    case EJobState::Completed:
                        LOG_DEBUG("Job completed (JobId: %v, Address: %v)",
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

TJobId TChunkReplicator::GenerateJobId()
{
    return MakeRandomId(EObjectType::MasterJob, Bootstrap_->GetCellTag());
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

    int replicationFactor = chunk->ComputeReplicationFactor();
    auto statistics = ComputeChunkStatistics(chunk);
    int replicaCount = statistics.ReplicaCount[index];
    int decommissionedReplicaCount = statistics.DecommissionedReplicaCount[index];

    if (replicaCount + decommissionedReplicaCount == 0) {
        return true;
    }

    if (replicaCount > replicationFactor) {
        return true;
    }

    int replicasNeeded;
    if (Any(statistics.Status & EChunkStatus::Underreplicated)) {
        replicasNeeded = replicationFactor - replicaCount;
    } else if (Any(statistics.Status & EChunkStatus::UnsafelyPlaced)) {
        replicasNeeded = 1;
    } else {
        return true;
    }

    // TODO(babenko): journal replication currently does not support fan-out > 1
    if (chunk->IsJournal()) {
        replicasNeeded = 1;
    }

    auto targetNodes = ChunkPlacement_->AllocateWriteTargets(
        chunk,
        replicasNeeded,
        1,
        Null,
        ESessionType::Replication);
    if (targetNodes.empty()) {
        return false;
    }

    *job = TJob::CreateReplicate(
        GenerateJobId(),
        chunkWithIndex,
        sourceNode,
        targetNodes);

    LOG_DEBUG("Replication job scheduled (JobId: %v, Address: %v, ChunkId: %v, TargetAddresses: %v)",
        (*job)->GetJobId(),
        sourceNode->GetDefaultAddress(),
        chunkWithIndex,
        MakeFormattableRange(targetNodes, TNodePtrAddressFormatter()));

    return targetNodes.size() == replicasNeeded;
}

bool TChunkReplicator::CreateBalancingJob(
    TNode* sourceNode,
    TChunkPtrWithIndex chunkWithIndex,
    double maxFillFactor,
    TJobPtr* job)
{
    auto* chunk = chunkWithIndex.GetPtr();
    if (chunk->GetRefreshScheduled()) {
        return true;
    }

    auto* targetNode = ChunkPlacement_->AllocateBalancingTarget(chunk, maxFillFactor);
    if (!targetNode) {
        return false;
    }

    *job = TJob::CreateReplicate(
        GenerateJobId(),
        chunkWithIndex,
        sourceNode,
        TNodeList(1, targetNode));

    LOG_DEBUG("Balancing job scheduled (JobId: %v, Address: %v, ChunkId: %v, TargetAddress: %v)",
        (*job)->GetJobId(),
        sourceNode->GetDefaultAddress(),
        chunkWithIndex,
        targetNode->GetDefaultAddress());

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
    if (IsObjectAlive(chunk)) {
        if (chunk->GetRefreshScheduled()) {
            return true;
        }
        if (HasRunningJobs(TChunkPtrWithIndex(chunk, chunkIdWithIndex.Index))) {
            return true;
        }
    }

    *job = TJob::CreateRemove(
        GenerateJobId(),
        chunkIdWithIndex,
        node);

    LOG_DEBUG("Removal job scheduled (JobId: %v, Address: %v, ChunkId: %v)",
        (*job)->GetJobId(),
        node->GetDefaultAddress(),
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

    auto targetNodes = ChunkPlacement_->AllocateWriteTargets(
        chunk,
        erasedIndexCount,
        erasedIndexCount,
        Null,
        ESessionType::Repair);
    if (targetNodes.empty()) {
        return false;
    }

    *job = TJob::CreateRepair(
        GenerateJobId(),
        chunk,
        node,
        targetNodes,
        erasedIndexes,
        Config_->RepairJobMemoryUsage);

    LOG_DEBUG("Repair job scheduled (JobId: %v, Address: %v, ChunkId: %v, TargetAddresses: %v, ErasedIndexes: %v)",
        (*job)->GetJobId(),
        node->GetDefaultAddress(),
        chunk->GetId(),
        MakeFormattableRange(targetNodes, TNodePtrAddressFormatter()),
        erasedIndexes);

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

    *job = TJob::CreateSeal(
        GenerateJobId(),
        chunk,
        node);

    LOG_DEBUG("Seal job scheduled (JobId: %v, Address: %v, ChunkId: %v)",
        (*job)->GetJobId(),
        node->GetDefaultAddress(),
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

    auto registerJob = [&] (TJobPtr job) {
        if (job) {
            resourceUsage += job->ResourceUsage();
            jobsToStart->push_back(job);
            RegisterJob(std::move(job));
        }
    };

    // NB: Beware of chunks larger than the limit; we still need to be able to replicate them one by one.
    auto hasSpareReplicationResources = [&] () {
        return
            resourceUsage.replication_slots() < resourceLimits.replication_slots() &&
            (resourceUsage.replication_slots() == 0 || resourceUsage.replication_data_size() < resourceLimits.replication_data_size());
    };

    // NB: Beware of chunks larger than the limit; we still need to be able to repair them one by one.
    auto hasSpareRepairResources = [&] () {
        return
            resourceUsage.repair_slots() < resourceLimits.repair_slots() &&
            (resourceUsage.repair_slots() == 0 || resourceUsage.repair_data_size() < resourceLimits.repair_data_size());
    };

    auto hasSpareSealResources = [&] () {
        return resourceUsage.seal_slots() < resourceLimits.seal_slots();
    };

    if (IsEnabled()) {
        // Schedule replication jobs.
        for (auto& queue : node->ChunkReplicationQueues()) {
            auto it = queue.begin();
            while (it != queue.end() && hasSpareReplicationResources()) {
                auto jt = it++;
                auto chunkWithIndex = *jt;

                TJobPtr job;
                if (CreateReplicationJob(node, chunkWithIndex, &job)) {
                    queue.erase(jt);
                }
                registerJob(std::move(job));
            }
        }

        // Schedule repair jobs.
        {
            auto it = ChunkRepairQueue_.begin();
            while (it != ChunkRepairQueue_.end() && hasSpareRepairResources()) {
                auto jt = it++;
                auto* chunk = *jt;

                TJobPtr job;
                if (CreateRepairJob(node, chunk, &job)) {
                    chunk->SetRepairQueueIterator(Null);
                    ChunkRepairQueue_.erase(jt);
                }
                registerJob(std::move(job));
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
                registerJob(std::move(job));
            }
        }

        // Schedule balancing jobs.
        double sourceFillFactor = ChunkPlacement_->GetFillFactor(node);
        double targetFillFactor = sourceFillFactor - Config_->MinBalancingFillFactorDiff;
        if (hasSpareReplicationResources() &&
            sourceFillFactor > Config_->MinBalancingFillFactor &&
            ChunkPlacement_->HasBalancingTargets(targetFillFactor))
        {
            int maxJobs = std::max(0, resourceLimits.replication_slots() - resourceUsage.replication_slots());
            auto chunksToBalance = ChunkPlacement_->GetBalancingChunks(node, maxJobs);
            for (auto chunkWithIndex : chunksToBalance) {
                if (!hasSpareReplicationResources())
                    break;

                TJobPtr job;
                CreateBalancingJob(node, chunkWithIndex, targetFillFactor, &job);
                registerJob(std::move(job));
            }
        }
    }

    // Schedule seal jobs.
    // NB: This feature is active regardless of replicator state.
    {
        auto& queue = node->ChunkSealQueue();
        auto it = queue.begin();
        while (it != queue.end() && hasSpareSealResources()) {
            auto jt = it++;
            auto* chunk = *jt;

            TJobPtr job;
            if (CreateSealJob(node, chunk, &job)) {
                queue.erase(jt);
            }
            registerJob(std::move(job));
        }
    }
}

void TChunkReplicator::RefreshChunk(TChunk* chunk)
{
    if (!chunk->IsConfirmed())
        return;

    ResetChunkStatus(chunk);

    auto statistics = ComputeChunkStatistics(chunk);

    if (Any(statistics.Status & EChunkStatus::Lost)) {
        YCHECK(LostChunks_.insert(chunk).second);
        if (chunk->ComputeVital() && (chunk->IsErasure() || chunk->ComputeReplicationFactor() > 1)) {
            YCHECK(LostVitalChunks_.insert(chunk).second);
        }
    }

    if (Any(statistics.Status & EChunkStatus::Overreplicated)) {
        YCHECK(OverreplicatedChunks_.insert(chunk).second);
    }

    if (Any(statistics.Status & EChunkStatus::Underreplicated)) {
        YCHECK(UnderreplicatedChunks_.insert(chunk).second);
    }

    if (Any(statistics.Status & EChunkStatus::DataMissing)) {
        YCHECK(DataMissingChunks_.insert(chunk).second);
    }

    if (Any(statistics.Status & EChunkStatus::ParityMissing)) {
        YCHECK(ParityMissingChunks_.insert(chunk).second);
    }

    if (Any(statistics.Status & EChunkStatus::QuorumMissing)) {
        YCHECK(QuorumMissingChunks_.insert(chunk).second);
    }

    if (Any(statistics.Status & EChunkStatus::UnsafelyPlaced)) {
        YCHECK(UnsafelyPlacedChunks_.insert(chunk).second);
    }

    if (!HasRunningJobs(chunk)) {
        RemoveChunkFromQueues(chunk, true);

        if (Any(statistics.Status & EChunkStatus::Overreplicated)) {
            for (auto nodeWithIndex : statistics.DecommissionedRemovalReplicas) {
                int index = nodeWithIndex.GetIndex();
                TChunkIdWithIndex chunkIdWithIndex(chunk->GetId(), index);
                auto* node = nodeWithIndex.GetPtr();
                if (node->GetLocalState() == ENodeState::Online) {
                    node->AddToChunkRemovalQueue(chunkIdWithIndex);
                }
            }

            for (int index : statistics.BalancingRemovalIndexes) {
                TChunkPtrWithIndex chunkWithIndex(chunk, index);
                TChunkIdWithIndex chunkIdWithIndex(chunk->GetId(), index);
                auto* targetNode = ChunkPlacement_->GetRemovalTarget(chunkWithIndex);
                if (targetNode) {
                    targetNode->AddToChunkRemovalQueue(chunkIdWithIndex);
                }
            }
        }

        if (Any(statistics.Status & (EChunkStatus::Underreplicated | EChunkStatus::UnsafelyPlaced))) {
            for (int index : statistics.ReplicationIndexes) {
                TChunkPtrWithIndex chunkWithIndex(chunk, index);
                TChunkIdWithIndex chunkIdWithIndex(chunk->GetId(), index);

                // Cap replica count minus one against the range [0, ReplicationPriorityCount - 1].
                int replicaCount = statistics.ReplicaCount[index];
                int priority = std::max(std::min(replicaCount - 1, ReplicationPriorityCount - 1), 0);

                for (auto replica : chunk->StoredReplicas()) {
                    if (chunk->IsRegular() ||
                        chunk->IsErasure() && replica.GetIndex() == index ||
                        chunk->IsJournal() && replica.GetIndex() == SealedChunkReplicaIndex)
                    {
                        auto* node = replica.GetPtr();
                        if (node->GetLocalState() == ENodeState::Online) {
                            node->AddToChunkReplicationQueue(chunkWithIndex, priority);
                        }
                    }
                }
            }
        }

        if (Any(statistics.Status & EChunkStatus::Sealed)) {
            YASSERT(chunk->IsJournal());
            for (auto replica : chunk->StoredReplicas()) {
                if (replica.GetIndex() == UnsealedChunkReplicaIndex) {
                    auto* node = replica.GetPtr();
                    if (node->GetLocalState() == ENodeState::Online) {
                        node->AddToChunkSealQueue(chunk);
                    }
                }
            }
        }

        if (Any(statistics.Status & (EChunkStatus::DataMissing | EChunkStatus::ParityMissing)) &&
            None(statistics.Status & EChunkStatus::Lost))
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

void TChunkReplicator::RemoveChunkFromQueues(TChunk* chunk, bool dropRemovals)
{
    for (auto nodeWithIndex : chunk->StoredReplicas()) {
        RemoveReplicaFromQueues(chunk, nodeWithIndex, dropRemovals);
    }

    if (chunk->IsErasure()) {
        RemoveFromChunkRepairQueue(chunk);
    }
}

void TChunkReplicator::RemoveReplicaFromQueues(TChunk* chunk, TNodePtrWithIndex nodeWithIndex, bool dropRemovals)
{
    auto* node = nodeWithIndex.GetPtr();
    TChunkPtrWithIndex chunkWithIndex(chunk, nodeWithIndex.GetIndex());
    TChunkIdWithIndex chunkIdWithIndex(chunk->GetId(), nodeWithIndex.GetIndex());
    if (dropRemovals) {
        node->RemoveFromChunkRemovalQueue(chunkIdWithIndex);
    }
    node->RemoveFromChunkReplicationQueues(chunkWithIndex);
    node->RemoveFromChunkSealQueue(chunk);
}

void TChunkReplicator::CancelChunkJobs(TChunk* chunk)
{
    auto it = JobListMap_.find(chunk);
    if (it == JobListMap_.end())
        return;

    auto jobList = it->second;
    for (auto job : jobList->Jobs()) {
        UnregisterJob(job, EJobUnregisterFlags::UnregisterFromNode);
    }
    JobListMap_.erase(it);
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
    ScheduleChunkRefresh(chunk);
}

void TChunkReplicator::ScheduleChunkRefresh(TChunk* chunk)
{
    if (!IsObjectAlive(chunk) ||
        chunk->GetRefreshScheduled() ||
        chunk->IsForeign())
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

    int totalCount = 0;
    int aliveCount = 0;
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
            ++totalCount;

            if (IsObjectAlive(chunk)) {
                ++aliveCount;
                chunk->SetRefreshScheduled(false);
                RefreshChunk(chunk);
            }

            objectManager->WeakUnrefObject(chunk);
        }
    }

    LOG_DEBUG("Incremental chunk refresh completed (TotalCount: %v, AliveCount: %v)",
        totalCount,
        aliveCount);
}

bool TChunkReplicator::IsEnabled()
{
    return Enabled_;
}

void TChunkReplicator::OnCheckEnabled()
{
    auto worldInitializer = Bootstrap_->GetWorldInitializer();
    if (!worldInitializer->CheckInitialized()) {
        Enabled_ = false;
        return;
    }

    try {
        if (Bootstrap_->IsPrimaryMaster()) {
            OnCheckEnabledPrimary();
        } else {
            OnCheckEnabledSecondary();
        }
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error updating replicator state, disabling until the next attempt");
        Enabled_ = false;
    }
}

void TChunkReplicator::OnCheckEnabledPrimary()
{
    auto cypressManager = Bootstrap_->GetCypressManager();
    auto resolver = cypressManager->CreateResolver();
    auto sysNode = resolver->ResolvePath("//sys");
    if (sysNode->Attributes().Get<bool>("disable_chunk_replicator", false)) {
        if (Enabled_) {
            LOG_INFO("Chunk replicator is disabled by //sys/@disable_chunk_replicator setting");
        }
        Enabled_ = false;
        return;
    }

    auto nodeTracker = Bootstrap_->GetNodeTracker();
    int needOnline = Config_->SafeOnlineNodeCount;
    int gotOnline = nodeTracker->GetOnlineNodeCount();
    if (gotOnline < needOnline) {
        if (Enabled_) {
            LOG_INFO("Chunk replicator disabled: too few online nodes, needed >= %v but got %v",
                needOnline,
                gotOnline);
        }
        Enabled_ = false;
        return;
    }

    auto multicellManager = Bootstrap_->GetMulticellManager();
    auto statistics = multicellManager->ComputeClusterStatistics();
    int gotChunkCount = statistics.chunk_count();
    int gotLostChunkCount = statistics.lost_vital_chunk_count();
    int needLostChunkCount = Config_->SafeLostChunkCount;
    if (gotChunkCount > 0) {
        double needFraction = Config_->SafeLostChunkFraction;
        double gotFraction = (double) gotLostChunkCount / gotChunkCount;
        if (gotFraction > needFraction) {
            if (Enabled_) {
                LOG_INFO("Chunk replicator disabled: too many lost chunks, fraction needed <= %v but got %v",
                    needFraction,
                    gotFraction);
            }
            Enabled_ = false;
            return;
        }
    }

    if (gotLostChunkCount > needLostChunkCount) {
        if (Enabled_) {
            LOG_INFO("Chunk replicator disabled: too many lost chunks, needed <= %v but got %v",
                needLostChunkCount,
                gotLostChunkCount);
        }
        Enabled_ = false;
        return;
    }

    if (!Enabled_) {
        LOG_INFO("Chunk replicator enabled");
    }
    Enabled_ = true;
}

void TChunkReplicator::OnCheckEnabledSecondary()
{
    auto multicellManager = Bootstrap_->GetMulticellManager();
    auto channel = multicellManager->GetMasterChannelOrThrow(Bootstrap_->GetPrimaryCellTag(), EPeerKind::Leader);
    TObjectServiceProxy proxy(channel);

    auto req = TYPathProxy::Get("//sys/@chunk_replicator_enabled");
    auto rsp = WaitFor(proxy.Execute(req))
        .ValueOrThrow();

    auto value = ConvertTo<bool>(TYsonString(rsp->value()));
    if (value != Enabled_) {
        if (value) {
            LOG_INFO("Chunk replicator enabled at primary master");
        } else {
            LOG_INFO("Chunk replicator disabled at primary master");
        }
        Enabled_ = value;
    }
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
            TChunkReplicatorPtr owner,
            TChunkList* root)
            : Bootstrap_(bootstrap)
            , Owner_(std::move(owner))
            , Root_(root)
        { }

        void Run()
        {
            TraverseChunkTree(CreatePreemptableChunkTraverserCallbacks(Bootstrap_), this, Root_);
        }

    private:
        TBootstrap* const Bootstrap_;
        const TChunkReplicatorPtr Owner_;
        TChunkList* const Root_;

        virtual bool OnChunk(
            TChunk* chunk,
            i64 /*rowIndex*/,
            const TReadLimit& /*startLimit*/,
            const TReadLimit& /*endLimit*/) override
        {
            Owner_->SchedulePropertiesUpdate(chunk);
            return true;
        }

        virtual void OnError(const TError& /*error*/) override
        { }

        virtual void OnFinish() override
        { }

    };

    New<TVisitor>(Bootstrap_, this, chunkList)->Run();
}

void TChunkReplicator::SchedulePropertiesUpdate(TChunk* chunk)
{
    if (!IsObjectAlive(chunk) ||
        chunk->GetPropertiesUpdateScheduled())
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
        return;
    }

    auto chunkManager = Bootstrap_->GetChunkManager();
    auto objectManager = Bootstrap_->GetObjectManager();

    TReqUpdateChunkProperties request;
    request.set_cell_tag(Bootstrap_->GetCellTag());

    // Extract up to MaxChunksPerPropertiesUpdate objects and post a mutation.
    int totalCount = 0;
    int aliveCount = 0;
    PROFILE_TIMING ("/properties_update_time") {
        for (int i = 0; i < Config_->MaxChunksPerPropertiesUpdate; ++i) {
            if (PropertiesUpdateList_.empty()) {
                break;
            }

            auto* chunk = PropertiesUpdateList_.front();
            PropertiesUpdateList_.pop_front();
            ++totalCount;

            if (IsObjectAlive(chunk)) {
                ++aliveCount;
                chunk->SetPropertiesUpdateScheduled(false);
                auto newProperties = ComputeChunkProperties(chunk);
                auto oldProperties = chunk->GetLocalProperties();
                if (newProperties != oldProperties) {
                    auto* update = request.add_updates();
                    ToProto(update->mutable_chunk_id(), chunk->GetId());
                    update->set_replication_factor(newProperties.ReplicationFactor);
                    update->set_vital(newProperties.Vital);
                }
            }

            objectManager->WeakUnrefObject(chunk);
        }
    }

    if (request.updates_size() == 0) {
        return;
    }

    LOG_DEBUG("Starting chunk properties update (TotalCount: %v, AliveCount: %v, UpdateCount: %v)",
        totalCount,
        aliveCount,
        request.updates_size());

    auto asyncResult = chunkManager
        ->CreateUpdateChunkPropertiesMutation(request)
        ->CommitAndLog(Logger);
    WaitFor(asyncResult);
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

    return parentsVisited ? properties : chunk->GetLocalProperties();
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

    LOG_DEBUG("Job registered (JobId: %v, JobType: %v, Address: %v)",
        job->GetJobId(),
        job->GetType(),
        job->GetNode()->GetDefaultAddress());
}

void TChunkReplicator::UnregisterJob(TJobPtr job, EJobUnregisterFlags flags)
{
    auto chunkManager = Bootstrap_->GetChunkManager();
    auto chunkId = job->GetChunkIdWithIndex().Id;
    auto* chunk = chunkManager->FindChunk(chunkId);

    YCHECK(JobMap_.erase(job->GetJobId()) == 1);

    if (Any(flags & EJobUnregisterFlags::UnregisterFromNode)) {
        YCHECK(job->GetNode()->Jobs().erase(job) == 1);
    }

    if (chunk) {
        if (Any(flags & EJobUnregisterFlags::UnregisterFromChunk)) {
            auto jobList = FindJobList(chunk);
            YCHECK(jobList);
            YCHECK(jobList->Jobs().erase(job) == 1);
            if (jobList->Jobs().empty()) {
                YCHECK(JobListMap_.erase(chunk) == 1);
            }
        }

        if (Any(flags & EJobUnregisterFlags::ScheduleChunkRefresh)) {
            ScheduleChunkRefresh(chunk);
        }
    }

    LOG_DEBUG("Job unregistered (JobId: %v, Address: %v)",
        job->GetJobId(),
        job->GetNode()->GetDefaultAddress());
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
