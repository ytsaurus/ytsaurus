#include "chunk_replicator.h"

#include "private.h"
#include "chunk.h"
#include "chunk_list.h"
#include "chunk_owner_base.h"
#include "chunk_placement.h"
#include "chunk_tree_traverser.h"
#include "chunk_view.h"
#include "config.h"
#include "job.h"
#include "job_registry.h"
#include "chunk_scanner.h"
#include "chunk_replica.h"
#include "medium.h"
#include "helpers.h"
#include "refresh_epoch.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/world_initializer.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/proto/multicell_manager.pb.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>

#include <yt/yt/server/master/cypress_server/node.h>
#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/node_tracker_server/data_center.h>
#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>
#include <yt/yt/server/master/node_tracker_server/rack.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>
#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/string.h>

#include <yt/yt/core/profiling/profiler.h>
#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <array>

namespace NYT::NChunkServer {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NHydra;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NNodeTrackerServer;
using namespace NObjectServer;
using namespace NChunkServer::NProto;
using namespace NCellMaster;
using namespace NTransactionClient;

using NChunkClient::TLegacyReadLimit;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkReplicator::TPerMediumChunkStatistics::TPerMediumChunkStatistics()
    : Status(EChunkStatus::None)
    , ReplicaCount{}
    , DecommissionedReplicaCount{}
{ }

////////////////////////////////////////////////////////////////////////////////

class TReplicationJob
    : public TJob
{
public:
    DEFINE_BYREF_RO_PROPERTY(TNodePtrWithIndexesList, TargetReplicas);
    DEFINE_BYVAL_RO_PROPERTY(TNodeId, TargetNodeId);

public:
    TReplicationJob(
        TJobId jobId,
        TJobEpoch jobEpoch,
        TNode* node,
        TChunkPtrWithIndexes chunkWithIndexes,
        const TNodePtrWithIndexesList& targetReplicas,
        TNodeId targetNodeId)
        : TJob(
            jobId,
            EJobType::ReplicateChunk,
            jobEpoch,
            node,
            TReplicationJob::GetResourceUsage(chunkWithIndexes.GetPtr()),
            ToChunkIdWithIndexes(chunkWithIndexes))
        , TargetReplicas_(targetReplicas)
        , TargetNodeId_(targetNodeId)
    { }

    void FillJobSpec(TBootstrap* /*bootstrap*/, TJobSpec* jobSpec) const override
    {
        auto* jobSpecExt = jobSpec->MutableExtension(TReplicateChunkJobSpecExt::replicate_chunk_job_spec_ext);
        ToProto(jobSpecExt->mutable_chunk_id(), EncodeChunkId(ChunkIdWithIndexes_));
        jobSpecExt->set_source_medium_index(ChunkIdWithIndexes_.MediumIndex);

        NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());
        for (auto replica : TargetReplicas_) {
            jobSpecExt->add_target_replicas(ToProto<ui64>(replica));
            builder.Add(replica);
        }
    }

private:
    static TNodeResources GetResourceUsage(TChunk* chunk)
    {
        auto dataSize = chunk->GetPartDiskSpace();

        TNodeResources resourceUsage;
        resourceUsage.set_replication_slots(1);
        resourceUsage.set_replication_data_size(dataSize);

        return resourceUsage;
    }
};

DEFINE_REFCOUNTED_TYPE(TReplicationJob)

////////////////////////////////////////////////////////////////////////////////

class TRemovalJob
    : public TJob
{
public:
    TRemovalJob(
        TJobId jobId,
        TJobEpoch jobEpoch,
        TNode* node,
        TChunk* chunk,
        const TChunkIdWithIndexes& chunkIdWithIndexes)
        : TJob(
            jobId,
            EJobType::RemoveChunk,
            jobEpoch,
            node,
            TRemovalJob::GetResourceUsage(),
            chunkIdWithIndexes)
        , Chunk_(chunk)
    { }

    void FillJobSpec(TBootstrap* bootstrap, TJobSpec* jobSpec) const override
    {
        auto* jobSpecExt = jobSpec->MutableExtension(TRemoveChunkJobSpecExt::remove_chunk_job_spec_ext);
        ToProto(jobSpecExt->mutable_chunk_id(), EncodeChunkId(ChunkIdWithIndexes_));
        jobSpecExt->set_medium_index(ChunkIdWithIndexes_.MediumIndex);
        if (!Chunk_) {
            jobSpecExt->set_chunk_is_dead(true);
            return;
        }

        bool isErasure = Chunk_->IsErasure();
        for (auto replica : Chunk_->StoredReplicas()) {
            if (replica.GetPtr()->GetDefaultAddress() == NodeAddress_) {
                continue;
            }
            if (isErasure && replica.GetReplicaIndex() != ChunkIdWithIndexes_.ReplicaIndex) {
                continue;
            }
            jobSpecExt->add_replicas(ToProto<ui32>(replica));
        }

        const auto& configManager = bootstrap->GetConfigManager();
        const auto& config = configManager->GetConfig()->ChunkManager;
        auto chunkRemovalJobExpirationDeadline = TInstant::Now() + config->ChunkRemovalJobReplicasExpirationTime;

        jobSpecExt->set_replicas_expiration_deadline(::NYT::ToProto<ui64>(chunkRemovalJobExpirationDeadline));
    }

private:
    TChunk* const Chunk_;

    static TNodeResources GetResourceUsage()
    {
        TNodeResources resourceUsage;
        resourceUsage.set_removal_slots(1);

        return resourceUsage;
    }
};

DEFINE_REFCOUNTED_TYPE(TRemovalJob)

////////////////////////////////////////////////////////////////////////////////

class TRepairJob
    : public TJob
{
public:
    DEFINE_BYREF_RO_PROPERTY(TNodePtrWithIndexesList, TargetReplicas);

public:
    TRepairJob(
        TJobId jobId,
        TJobEpoch jobEpoch,
        TNode* node,
        i64 jobMemoryUsage,
        TChunk* chunk,
        const TNodePtrWithIndexesList& targetReplicas,
        bool decommission)
        : TJob(
            jobId,
            EJobType::RepairChunk,
            jobEpoch,
            node,
            TRepairJob::GetResourceUsage(chunk, jobMemoryUsage),
            TChunkIdWithIndexes{chunk->GetId(), GenericChunkReplicaIndex, GenericMediumIndex})
        , TargetReplicas_(targetReplicas)
        , Chunk_(chunk)
        , Decommission_(decommission)
    { }

    void FillJobSpec(TBootstrap* /*bootstrap*/, TJobSpec* jobSpec) const override
    {
        auto* jobSpecExt = jobSpec->MutableExtension(TRepairChunkJobSpecExt::repair_chunk_job_spec_ext);
        jobSpecExt->set_erasure_codec(static_cast<int>(Chunk_->GetErasureCodec()));
        ToProto(jobSpecExt->mutable_chunk_id(), Chunk_->GetId());
        jobSpecExt->set_decommission(Decommission_);

        if (Chunk_->IsJournal()) {
            YT_VERIFY(Chunk_->IsSealed());
            jobSpecExt->set_row_count(Chunk_->GetPhysicalSealedRowCount());
        }

        NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());

        const auto& sourceReplicas = Chunk_->StoredReplicas();
        builder.Add(sourceReplicas);
        ToProto(jobSpecExt->mutable_source_replicas(), sourceReplicas);

        jobSpecExt->set_striped_erasure_chunk(Chunk_->GetStripedErasure());

        for (auto replica : TargetReplicas_) {
            jobSpecExt->add_target_replicas(ToProto<ui64>(replica));
            builder.Add(replica);
        }
    }

private:
    TChunk* const Chunk_;
    const bool Decommission_;

    static TNodeResources GetResourceUsage(TChunk* chunk, i64 jobMemoryUsage)
    {
        auto dataSize = chunk->GetPartDiskSpace();

        TNodeResources resourceUsage;
        resourceUsage.set_repair_slots(1);
        resourceUsage.set_system_memory(jobMemoryUsage);
        resourceUsage.set_repair_data_size(dataSize);

        return resourceUsage;
    }
};

DEFINE_REFCOUNTED_TYPE(TRepairJob)

////////////////////////////////////////////////////////////////////////////////

TChunkReplicator::TChunkReplicator(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap,
    TChunkPlacementPtr chunkPlacement,
    IJobRegistryPtr jobRegistry)
    : Config_(config)
    , Bootstrap_(bootstrap)
    , ChunkPlacement_(std::move(chunkPlacement))
    , JobRegistry_(std::move(jobRegistry))
    , BlobRefreshScanner_(std::make_unique<TChunkScanner>(
        Bootstrap_->GetObjectManager(),
        EChunkScanKind::Refresh,
        /*journal*/ false))
    , JournalRefreshScanner_(std::make_unique<TChunkScanner>(
        Bootstrap_->GetObjectManager(),
        EChunkScanKind::Refresh,
        /*journal*/ true))
    , BlobRequisitionUpdateScanner_(std::make_unique<TChunkScanner>(
        Bootstrap_->GetObjectManager(),
        EChunkScanKind::RequisitionUpdate,
        /*journal*/ false))
    , JournalRequisitionUpdateScanner_(std::make_unique<TChunkScanner>(
        Bootstrap_->GetObjectManager(),
        EChunkScanKind::RequisitionUpdate,
        /*journal*/ true))
    , MissingPartChunkRepairQueueBalancer_(
        Config_->RepairQueueBalancerWeightDecayFactor,
        Config_->RepairQueueBalancerWeightDecayInterval)
    , DecommissionedPartChunkRepairQueueBalancer_(
        Config_->RepairQueueBalancerWeightDecayFactor,
        Config_->RepairQueueBalancerWeightDecayInterval)
{
    for (int i = 0; i < MaxMediumCount; ++i) {
        // We "balance" medium indexes, not the repair queues themselves.
        MissingPartChunkRepairQueueBalancer_.AddContender(i);
        DecommissionedPartChunkRepairQueueBalancer_.AddContender(i);
    }

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(BIND(&TChunkReplicator::OnDynamicConfigChanged, MakeWeak(this)));

    std::fill(JobEpochs_.begin(), JobEpochs_.end(), InvalidJobEpoch);
    for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
        SetRefreshEpoch(shardIndex, InvalidRefreshEpoch);
    }
}

TChunkReplicator::~TChunkReplicator()
{ }

void TChunkReplicator::Start()
{
    for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
        StartRefresh(shardIndex);
    }
    StartRequisitionUpdate();

    RefreshExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkRefresher),
        BIND(&TChunkReplicator::OnRefresh, MakeWeak(this)),
        GetDynamicConfig()->ChunkRefreshPeriod);
    RefreshExecutor_->Start();

    RequisitionUpdateExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkRequisitionUpdater),
        BIND(&TChunkReplicator::OnRequisitionUpdate, MakeWeak(this)),
        GetDynamicConfig()->ChunkRequisitionUpdatePeriod);
    RequisitionUpdateExecutor_->Start();

    FinishedRequisitionTraverseFlushExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkRequisitionUpdater),
        BIND(&TChunkReplicator::OnFinishedRequisitionTraverseFlush, MakeWeak(this)),
        GetDynamicConfig()->FinishedChunkListsRequisitionTraverseFlushPeriod);
    FinishedRequisitionTraverseFlushExecutor_->Start();

    EnabledCheckExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Periodic),
        BIND(&TChunkReplicator::OnCheckEnabled, MakeWeak(this)),
        Config_->ReplicatorEnabledCheckPeriod);
    EnabledCheckExecutor_->Start();

    // Just in case.
    Enabled_ = false;
}

void TChunkReplicator::Stop()
{
    for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
        StopRefresh(shardIndex);
    }
    StopRequisitionUpdate();

    ClearChunkRequisitionCache();
    TmpRequisitionRegistry_.Clear();

    LastPerNodeProfilingTime_ = TInstant::Zero();

    if (RefreshExecutor_) {
        RefreshExecutor_->Stop();
        RefreshExecutor_.Reset();
    }

    if (RequisitionUpdateExecutor_) {
        RequisitionUpdateExecutor_->Stop();
        RequisitionUpdateExecutor_.Reset();
    }

    if (FinishedRequisitionTraverseFlushExecutor_) {
        FinishedRequisitionTraverseFlushExecutor_->Stop();
        FinishedRequisitionTraverseFlushExecutor_.Reset();
    }

    if (EnabledCheckExecutor_) {
        EnabledCheckExecutor_->Stop();
        EnabledCheckExecutor_.Reset();
    }

    ChunkListIdsWithFinishedRequisitionTraverse_.clear();

    for (auto& queue : MissingPartChunkRepairQueues_) {
        for (auto chunkWithIndexes : queue) {
            chunkWithIndexes.GetPtr()->SetRepairQueueIterator(
                chunkWithIndexes.GetMediumIndex(),
                EChunkRepairQueue::Missing,
                TChunkRepairQueueIterator());
        }
        queue.clear();
    }
    MissingPartChunkRepairQueueBalancer_.ResetWeights();

    for (auto& queue : DecommissionedPartChunkRepairQueues_) {
        for (auto chunkWithIndexes : queue) {
            chunkWithIndexes.GetPtr()->SetRepairQueueIterator(
                chunkWithIndexes.GetMediumIndex(),
                EChunkRepairQueue::Decommissioned,
                TChunkRepairQueueIterator());
        }
        queue.clear();
    }
    DecommissionedPartChunkRepairQueueBalancer_.ResetWeights();

    ChunkIdsPendingEndorsementRegistration_.clear();

    for (auto jobType : TEnumTraits<EJobType>::GetDomainValues()) {
        MisscheduledJobs_[jobType] = 0;
    }

    Enabled_ = false;
}

void TChunkReplicator::TouchChunk(TChunk* chunk)
{
    // Fast path.
    if (!ShouldProcessChunk(chunk)) {
        return;
    }

    const auto replication = GetChunkAggregatedReplication(chunk);

    for (auto& entry : replication) {
        auto mediumIndex = entry.GetMediumIndex();
        for (auto queue : TEnumTraits<EChunkRepairQueue>::GetDomainValues()) {
            auto repairIt = chunk->GetRepairQueueIterator(mediumIndex, queue);
            if (repairIt == TChunkRepairQueueIterator()) {
                continue;
            }
            auto& chunkRepairQueue = ChunkRepairQueue(mediumIndex, queue);
            chunkRepairQueue.erase(repairIt);
            TChunkPtrWithIndexes chunkWithIndexes(chunk, GenericChunkReplicaIndex, mediumIndex);
            auto newRepairIt = chunkRepairQueue.insert(chunkRepairQueue.begin(), chunkWithIndexes);
            chunk->SetRepairQueueIterator(mediumIndex, queue, newRepairIt);
        }
    }
}

TMediumMap<EChunkStatus> TChunkReplicator::ComputeChunkStatuses(TChunk* chunk)
{
    TMediumMap<EChunkStatus> result;

    auto statistics = ComputeChunkStatistics(chunk);

    for (const auto& [mediumIndex, mediumStatistics] : statistics.PerMediumStatistics) {
        result[mediumIndex] = mediumStatistics.Status;
    }

    return result;
}

TChunkReplicator::TChunkStatistics TChunkReplicator::ComputeChunkStatistics(const TChunk* chunk)
{
    auto result = chunk->IsErasure()
        ? ComputeErasureChunkStatistics(chunk)
        : ComputeRegularChunkStatistics(chunk);

    if (chunk->IsJournal() && chunk->IsSealed()) {
        result.Status |= ECrossMediumChunkStatus::Sealed;
    }

    return result;
}

TChunkReplicator::TChunkStatistics TChunkReplicator::ComputeErasureChunkStatistics(const TChunk* chunk)
{
    TChunkStatistics result;

    auto* codec = NErasure::GetCodec(chunk->GetErasureCodec());

    TMediumMap<std::array<TNodePtrWithIndexesList, ChunkReplicaIndexBound>> decommissionedReplicas;
    TMediumMap<std::array<ui8, RackIndexBound>> perRackReplicaCounters;
    // TODO(gritukan): YT-16557.
    TMediumMap<THashMap<const TDataCenter*, ui8>> perDataCenterReplicaCounters;

    // An arbitrary replica collocated with too may others within a single rack - per medium.
    TMediumMap<TNodePtrWithIndexes> unsafelyPlacedSealedReplicas;

    TMediumIntMap totalReplicaCounts;
    TMediumIntMap totalDecommissionedReplicaCounts;

    NErasure::TPartIndexSet replicaIndexes;

    bool totallySealed = chunk->IsSealed();

    auto mark = TNode::GenerateVisitMark();

    const auto chunkReplication = GetChunkAggregatedReplication(chunk);
    for (const auto& entry : chunkReplication) {
        auto mediumIndex = entry.GetMediumIndex();
        unsafelyPlacedSealedReplicas[mediumIndex] = {};
        totalReplicaCounts[mediumIndex] = 0;
        totalDecommissionedReplicaCounts[mediumIndex] = 0;
    }

    for (auto replica : chunk->StoredReplicas()) {
        auto* node = replica.GetPtr();
        int replicaIndex = replica.GetReplicaIndex();
        int mediumIndex = replica.GetMediumIndex();
        auto& mediumStatistics = result.PerMediumStatistics[mediumIndex];

        replicaIndexes.set(replicaIndex);

        auto isReplicaSealed = !chunk->IsJournal() || replica.GetState() == EChunkReplicaState::Sealed;

        if (!isReplicaSealed) {
            totallySealed = false;
        }

        if (IsReplicaDecommissioned(replica) || node->GetVisitMark(mediumIndex) == mark) {
            ++mediumStatistics.DecommissionedReplicaCount[replicaIndex];
            decommissionedReplicas[mediumIndex][replicaIndex].push_back(replica);
            ++totalDecommissionedReplicaCounts[mediumIndex];
        } else {
            ++mediumStatistics.ReplicaCount[replicaIndex];
            ++totalReplicaCounts[mediumIndex];
        }

        if (!Config_->AllowMultipleErasurePartsPerNode) {
            node->SetVisitMark(mediumIndex, mark);
        }

        if (const auto* rack = node->GetRack()) {
            int rackIndex = rack->GetIndex();
            // An erasure chunk is considered placed unsafely if it violates replica limits
            // in any failure domain.
            auto maxReplicasPerRack = ChunkPlacement_->GetMaxReplicasPerRack(mediumIndex, chunk);
            auto replicasPerRack = ++perRackReplicaCounters[mediumIndex][rackIndex];
            if (replicasPerRack > maxReplicasPerRack && isReplicaSealed) {
                unsafelyPlacedSealedReplicas[mediumIndex] = replica;
            }

            if (auto* dataCenter = rack->GetDataCenter()) {
                auto maxReplicasPerDataCenter = ChunkPlacement_->GetMaxReplicasPerDataCenter(mediumIndex, chunk, dataCenter);
                auto replicasPerDataCenter = ++perDataCenterReplicaCounters[mediumIndex][dataCenter];
                if (replicasPerDataCenter > maxReplicasPerDataCenter && isReplicaSealed) {
                    unsafelyPlacedSealedReplicas[mediumIndex] = replica;
                }
            }
        }
    }

    bool allMediaTransient = true;
    bool allMediaDataPartsOnly = true;
    TMediumMap<NErasure::TPartIndexSet> mediumToErasedIndexes;
    TMediumSet activeMedia;

    const auto& chunkManager = Bootstrap_->GetChunkManager();

    for (const auto& entry : chunkReplication) {
        auto mediumIndex = entry.GetMediumIndex();
        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        YT_VERIFY(IsObjectAlive(medium));

        if (medium->GetCache()) {
            continue;
        }

        auto mediumTransient = medium->GetTransient();

        auto replicationPolicy = entry.Policy();

        auto dataPartsOnly = replicationPolicy.GetDataPartsOnly();
        auto mediumReplicationFactor = replicationPolicy.GetReplicationFactor();

        if (mediumReplicationFactor == 0 &&
            totalReplicaCounts[mediumIndex] == 0 &&
            totalDecommissionedReplicaCounts[mediumIndex] == 0)
        {
            // This medium is irrelevant to this chunk.
            continue;
        }

        allMediaTransient = allMediaTransient && mediumTransient;
        allMediaDataPartsOnly = allMediaDataPartsOnly && dataPartsOnly;

        activeMedia.set(mediumIndex);

        ComputeErasureChunkStatisticsForMedium(
            result.PerMediumStatistics[mediumIndex],
            codec,
            replicationPolicy,
            decommissionedReplicas[mediumIndex],
            unsafelyPlacedSealedReplicas[mediumIndex],
            mediumToErasedIndexes[mediumIndex],
            totallySealed);
    }

    ComputeErasureChunkStatisticsCrossMedia(
        result,
        chunk,
        codec,
        allMediaTransient,
        allMediaDataPartsOnly,
        mediumToErasedIndexes,
        activeMedia,
        replicaIndexes,
        totallySealed);

    return result;
}

TMediumMap<TNodeList> TChunkReplicator::GetChunkConsistentPlacementNodes(const TChunk* chunk)
{
    if (!chunk->HasConsistentReplicaPlacementHash()) {
        return {};
    }

    if (!IsConsistentChunkPlacementEnabled()) {
        return {};
    }

    const auto& chunkManager = Bootstrap_->GetChunkManager();

    TMediumMap<TNodeList> result;
    const auto chunkReplication = GetChunkAggregatedReplication(chunk);
    for (const auto& entry : chunkReplication) {
        auto mediumPolicy = entry.Policy();
        if (!mediumPolicy) {
            continue;
        }

        auto mediumIndex = entry.GetMediumIndex();
        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        YT_VERIFY(IsObjectAlive(medium));

        if (medium->GetCache()) {
            continue;
        }

        auto it = result.emplace(
            mediumIndex,
            ChunkPlacement_->GetConsistentPlacementWriteTargets(chunk, mediumIndex)).first;
        const auto& mediumConsistentPlacementNodes = it->second;

        if (mediumConsistentPlacementNodes.empty()) {
            // There no nodes; skip.
            result.erase(it);
            continue;
        }

        YT_VERIFY(
            std::ssize(mediumConsistentPlacementNodes) ==
            chunk->GetPhysicalReplicationFactor(mediumIndex, GetChunkRequisitionRegistry()));
    }

    return result;
}

void TChunkReplicator::ComputeErasureChunkStatisticsForMedium(
    TPerMediumChunkStatistics& result,
    NErasure::ICodec* codec,
    TReplicationPolicy replicationPolicy,
    const std::array<TNodePtrWithIndexesList, ChunkReplicaIndexBound>& decommissionedReplicas,
    TNodePtrWithIndexes unsafelyPlacedSealedReplica,
    NErasure::TPartIndexSet& erasedIndexes,
    bool totallySealed)
{
    int replicationFactor = replicationPolicy.GetReplicationFactor();
    YT_VERIFY(0 <= replicationFactor && replicationFactor <= 1);

    int totalPartCount = codec->GetTotalPartCount();
    int dataPartCount = codec->GetDataPartCount();

    for (int index = 0; index < totalPartCount; ++index) {
        int replicaCount = result.ReplicaCount[index];
        int decommissionedReplicaCount = result.DecommissionedReplicaCount[index];
        auto isDataPart = index < dataPartCount;
        auto removalAdvised = replicationFactor == 0 || (!isDataPart && replicationPolicy.GetDataPartsOnly());
        auto targetReplicationFactor = removalAdvised ? 0 : 1;

        if (totallySealed) {
            if (replicaCount >= targetReplicationFactor && decommissionedReplicaCount > 0) {

                // A replica may be "decommissioned" either because its node is
                // decommissioned or that node holds another part of the chunk (and that's
                // not allowed by the configuration).
                // TODO(shakurov): this is unintuitive. Express clashes explicitly.

                const auto& replicas = decommissionedReplicas[index];
                result.DecommissionedRemovalReplicas.insert(
                    result.DecommissionedRemovalReplicas.end(),
                    replicas.begin(),
                    replicas.end());
                result.Status |= EChunkStatus::Overreplicated;
            }

            if (replicaCount > targetReplicationFactor && decommissionedReplicaCount == 0) {
                result.Status |= EChunkStatus::Overreplicated;
                result.BalancingRemovalIndexes.push_back(index);
            }

            if (replicaCount == 0 && decommissionedReplicaCount > 0 && !removalAdvised && std::ssize(decommissionedReplicas) > index) {
                const auto& replicas = decommissionedReplicas[index];
                // A replica may be "decommissioned" either because its node is
                // decommissioned or that node holds another part of the chunk (and that's
                // not allowed by the configuration). Let's distinguish these cases.
                auto isReplicaDecommissioned = [&] (const TNodePtrWithIndexes& replica) {
                    return IsReplicaDecommissioned(replica);
                };
                if (std::all_of(replicas.begin(), replicas.end(), isReplicaDecommissioned)) {
                    result.Status |= (isDataPart ? EChunkStatus::DataDecommissioned : EChunkStatus::ParityDecommissioned);
                } else {
                    result.Status |= EChunkStatus::Underreplicated;
                    result.ReplicationIndexes.push_back(index);
                }
            }
        }

        if (replicaCount == 0 && decommissionedReplicaCount == 0 && !removalAdvised) {
            erasedIndexes.set(index);
            result.Status |= isDataPart ? EChunkStatus::DataMissing : EChunkStatus::ParityMissing;
        }
    }

    if (!codec->CanRepair(erasedIndexes) &&
        erasedIndexes.any()) // This is to avoid flagging chunks with no parity
                             // parts as lost when dataPartsOnly == true.
    {
        result.Status |= EChunkStatus::Lost;
    }

    if (unsafelyPlacedSealedReplica &&
        None(result.Status & EChunkStatus::Overreplicated))
    {
        result.Status |= EChunkStatus::UnsafelyPlaced;
        if (result.ReplicationIndexes.empty()) {
            result.ReplicationIndexes.push_back(unsafelyPlacedSealedReplica.GetReplicaIndex());
            result.UnsafelyPlacedReplica = unsafelyPlacedSealedReplica;
        }
    }
}

void TChunkReplicator::ComputeErasureChunkStatisticsCrossMedia(
    TChunkStatistics& result,
    const TChunk* chunk,
    NErasure::ICodec* codec,
    bool allMediaTransient,
    bool allMediaDataPartsOnly,
    const TMediumMap<NErasure::TPartIndexSet>& mediumToErasedIndexes,
    const TMediumSet& activeMedia,
    const NErasure::TPartIndexSet& replicaIndexes,
    bool totallySealed)
{
    if (!chunk->IsSealed() && static_cast<ssize_t>(replicaIndexes.count()) < chunk->GetReadQuorum()) {
        result.Status |= ECrossMediumChunkStatus::QuorumMissing;
    }

    // In contrast to regular chunks, erasure chunk being "lost" on every medium
    // doesn't mean it's lost for good: across all media, there still may be
    // enough parts to make it repairable.

    std::bitset<MaxMediumCount> transientMedia;
    if (allMediaTransient) {
        transientMedia.flip();
    } else {
        for (const auto& mediumIdAndPtrPair : Bootstrap_->GetChunkManager()->Media()) {
            auto* medium = mediumIdAndPtrPair.second;
            if (medium->GetCache()) {
                continue;
            }

            transientMedia.set(medium->GetIndex(), medium->GetTransient());
        }
    }

    NErasure::TPartIndexSet crossMediumErasedIndexes;
    // Erased indexes as they would look if all transient media were to disappear.
    NErasure::TPartIndexSet crossMediumErasedIndexesNoTransient;
    crossMediumErasedIndexes.flip();
    crossMediumErasedIndexesNoTransient.flip();

    static const NErasure::TPartIndexSet emptySet;

    auto deficient = false;
    for (int mediumIndex = 0; mediumIndex < MaxMediumCount; ++mediumIndex) {
        if (!activeMedia[mediumIndex]) {
            continue;
        }
        auto it = mediumToErasedIndexes.find(mediumIndex);
        const auto& erasedIndexes = it == mediumToErasedIndexes.end() ? emptySet : it->second;
        crossMediumErasedIndexes &= erasedIndexes;
        if (!transientMedia.test(mediumIndex)) {
            crossMediumErasedIndexesNoTransient &= erasedIndexes;
        }

        const auto& mediumStatistics = result.PerMediumStatistics[mediumIndex];
        if (Any(mediumStatistics.Status &
                (EChunkStatus::DataMissing | EChunkStatus::ParityMissing |
                 EChunkStatus::DataDecommissioned | EChunkStatus::ParityDecommissioned)))
        {
            deficient = true;
        }
    }

    int totalPartCount = codec->GetTotalPartCount();
    int dataPartCount = codec->GetDataPartCount();

    bool crossMediaDataMissing = false;
    bool crossMediaParityMissing = false;
    bool precarious = false;
    bool crossMediaLost = false;

    if (crossMediumErasedIndexes.any()) {
        for (int index = 0; index < dataPartCount; ++index) {
            if (crossMediumErasedIndexes.test(index)) {
                crossMediaDataMissing = true;
                break;
            }
        }
        for (int index = dataPartCount; index < totalPartCount; ++index) {
            if (crossMediumErasedIndexes.test(index)) {
                crossMediaParityMissing = true;
                break;
            }
        }

        crossMediaLost = !codec->CanRepair(crossMediumErasedIndexes);
    }

    if (!crossMediaLost && crossMediumErasedIndexesNoTransient.any()) {
        precarious = !codec->CanRepair(crossMediumErasedIndexesNoTransient);
    }

    if (crossMediaLost) {
        result.Status |= ECrossMediumChunkStatus::Lost;
    } else {
        for (const auto& mediumStatistics : result.PerMediumStatistics) {
            if (Any(mediumStatistics.second.Status & EChunkStatus::Lost)) {
                // The chunk is lost on at least one medium.
                result.Status |= ECrossMediumChunkStatus::MediumWiseLost;
                break;
            }
        }
    }

    if (deficient && None(result.Status & ECrossMediumChunkStatus::MediumWiseLost)) {
        result.Status |= ECrossMediumChunkStatus::Deficient;
    }
    if (crossMediaDataMissing) {
        result.Status |= ECrossMediumChunkStatus::DataMissing;
    }
    if (crossMediaParityMissing && !allMediaDataPartsOnly) {
        result.Status |= ECrossMediumChunkStatus::ParityMissing;
    }
    if (precarious && !allMediaTransient) {
        result.Status |= ECrossMediumChunkStatus::Precarious;
    }

    if (totallySealed) {
        // Replicate parts cross-media. Do this even if the chunk is unrepairable:
        // having identical states on all media is just simpler to reason about.
        for (const auto& [mediumIndex, erasedIndexes] : mediumToErasedIndexes) {
            auto& mediumStatistics = result.PerMediumStatistics[mediumIndex];

            for (int index = 0; index < totalPartCount; ++index) {
                if (erasedIndexes.test(index) && // If dataPartsOnly is true, everything beyond dataPartCount will test negative.
                    !crossMediumErasedIndexes.test(index))
                {
                    mediumStatistics.Status |= EChunkStatus::Underreplicated;
                    mediumStatistics.ReplicationIndexes.push_back(index);
                }
            }
        }
    }
}

TChunkReplicator::TChunkStatistics TChunkReplicator::ComputeRegularChunkStatistics(const TChunk* chunk)
{
    TChunkStatistics results;

    TMediumMap<TNodePtrWithIndexes> unsafelyPlacedReplicas;
    TMediumMap<std::array<ui8, RackIndexBound>> perRackReplicaCounters;
    // TODO(gritukan): YT-16557.
    TMediumMap<THashMap<const TDataCenter*, ui8>> perDataCenterReplicaCounters;

    // An arbitrary replica that violates consistent placement requirements - per medium.
    TMediumMap<TNodePtrWithIndexes> inconsistentlyPlacedReplica;

    TMediumIntMap replicaCount;
    TMediumIntMap decommissionedReplicaCount;
    TMediumMap<TNodePtrWithIndexesList> decommissionedReplicas;
    int totalReplicaCount = 0;
    int totalDecommissionedReplicaCount = 0;

    TMediumMap<TNodePtrWithIndexesList> missingReplicas;

    TMediumSet hasSealedReplica;
    bool hasSealedReplicas = false;
    bool totallySealed = chunk->IsSealed();

    auto consistentPlacementNodes = GetChunkConsistentPlacementNodes(chunk);
    auto storedReplicas = chunk->StoredReplicas();

    for (const auto& [mediumIndex, consistentNodes] : consistentPlacementNodes) {
        for (auto node : consistentNodes) {
            TNodePtrWithIndexes nodePtrWithIndexes(node, GenericChunkReplicaIndex, mediumIndex);
            // This is usually small, I'm fine with that.
            if (std::find(storedReplicas.begin(), storedReplicas.end(), nodePtrWithIndexes) == storedReplicas.end()) {
                missingReplicas[mediumIndex].push_back(nodePtrWithIndexes);
            }
        }
    }

    for (auto replica : storedReplicas) {
        auto* node = replica.GetPtr();
        auto mediumIndex = replica.GetMediumIndex();

        if (chunk->IsJournal() && replica.GetState() != EChunkReplicaState::Sealed) {
            totallySealed = false;
        } else {
            hasSealedReplica[mediumIndex] = true;
            hasSealedReplicas = true;
        }

        if (IsReplicaDecommissioned(replica)) {
            ++decommissionedReplicaCount[mediumIndex];
            decommissionedReplicas[mediumIndex].push_back(replica);
            ++totalDecommissionedReplicaCount;
        } else {
            ++replicaCount[mediumIndex];
            ++totalReplicaCount;
        }

        if (const auto* rack = replica.GetPtr()->GetRack()) {
            int rackIndex = rack->GetIndex();
            auto maxReplicasPerRack = ChunkPlacement_->GetMaxReplicasPerRack(mediumIndex, chunk);
            if (++perRackReplicaCounters[mediumIndex][rackIndex] > maxReplicasPerRack) {
                unsafelyPlacedReplicas[mediumIndex] = replica;
            }

            if (const auto* dataCenter = rack->GetDataCenter()) {
                auto maxReplicasPerDataCenter = ChunkPlacement_->GetMaxReplicasPerDataCenter(mediumIndex, chunk, dataCenter);
                if (++perDataCenterReplicaCounters[mediumIndex][dataCenter] > maxReplicasPerDataCenter) {
                    unsafelyPlacedReplicas[mediumIndex] = replica;
                }
            }
        }

        if (auto it = consistentPlacementNodes.find(mediumIndex); it != consistentPlacementNodes.end()) {
            const auto& mediumConsistentPlacementNodes = it->second;
            if (std::find(
                mediumConsistentPlacementNodes.begin(),
                mediumConsistentPlacementNodes.end(),
                node) == mediumConsistentPlacementNodes.end())
            {
                inconsistentlyPlacedReplica[mediumIndex] = TNodePtrWithIndexes(node, GenericChunkReplicaIndex, mediumIndex);
            }
        }
    }

    bool precarious = true;
    bool allMediaTransient = true;
    TCompactVector<int, MaxMediumCount> mediaOnWhichLost;
    bool hasMediumOnWhichPresent = false;
    bool hasMediumOnWhichUnderreplicated = false;
    bool hasMediumOnWhichSealedMissing = false;

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto replication = GetChunkAggregatedReplication(chunk);
    for (auto entry : replication) {
        auto mediumIndex = entry.GetMediumIndex();
        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        YT_VERIFY(IsObjectAlive(medium));

        if (medium->GetCache()) {
            continue;
        }

        auto& mediumStatistics = results.PerMediumStatistics[mediumIndex];
        auto mediumTransient = medium->GetTransient();

        auto mediumReplicationPolicy = entry.Policy();
        auto mediumReplicaCount = replicaCount[mediumIndex];
        auto mediumDecommissionedReplicaCount = decommissionedReplicaCount[mediumIndex];

        // NB: some very counter-intuitive scenarios are possible here.
        // E.g. mediumReplicationFactor == 0, but mediumReplicaCount != 0.
        // This happens when chunk's requisition changes. One should be careful
        // with one's assumptions.
        if (!mediumReplicationPolicy &&
            mediumReplicaCount == 0 &&
            mediumDecommissionedReplicaCount == 0)
        {
            // This medium is irrelevant to this chunk.
            continue;
        }

        ComputeRegularChunkStatisticsForMedium(
            mediumStatistics,
            chunk,
            mediumReplicationPolicy,
            mediumReplicaCount,
            mediumDecommissionedReplicaCount,
            decommissionedReplicas[mediumIndex],
            hasSealedReplica[mediumIndex],
            totallySealed,
            unsafelyPlacedReplicas[mediumIndex],
            inconsistentlyPlacedReplica[mediumIndex],
            missingReplicas[mediumIndex]);

        allMediaTransient = allMediaTransient && mediumTransient;

        if (Any(mediumStatistics.Status & EChunkStatus::Underreplicated)) {
            hasMediumOnWhichUnderreplicated = true;
        }

        if (Any(mediumStatistics.Status & EChunkStatus::SealedMissing)) {
            hasMediumOnWhichSealedMissing = true;
        }

        if (Any(mediumStatistics.Status & EChunkStatus::Lost)) {
            mediaOnWhichLost.push_back(mediumIndex);
        } else {
            hasMediumOnWhichPresent = true;
            precarious = precarious && mediumTransient;
        }
    }

    ComputeRegularChunkStatisticsCrossMedia(
        results,
        chunk,
        totalReplicaCount,
        totalDecommissionedReplicaCount,
        hasSealedReplicas,
        precarious,
        allMediaTransient,
        mediaOnWhichLost,
        hasMediumOnWhichPresent,
        hasMediumOnWhichUnderreplicated,
        hasMediumOnWhichSealedMissing);

    return results;
}

void TChunkReplicator::ComputeRegularChunkStatisticsForMedium(
    TPerMediumChunkStatistics& result,
    const TChunk* chunk,
    TReplicationPolicy replicationPolicy,
    int replicaCount,
    int decommissionedReplicaCount,
    const TNodePtrWithIndexesList& decommissionedReplicas,
    bool hasSealedReplica,
    bool totallySealed,
    TNodePtrWithIndexes unsafelyPlacedReplica,
    TNodePtrWithIndexes inconsistentlyPlacedReplica,
    const TNodePtrWithIndexesList& missingReplicas)
{
    auto replicationFactor = replicationPolicy.GetReplicationFactor();

    result.ReplicaCount[GenericChunkReplicaIndex] = replicaCount;
    result.DecommissionedReplicaCount[GenericChunkReplicaIndex] = decommissionedReplicaCount;
    result.MissingReplicas = missingReplicas;

    if (replicaCount + decommissionedReplicaCount == 0) {
        result.Status |= EChunkStatus::Lost;
    }

    if (chunk->IsSealed()) {
        if (chunk->IsJournal() && replicationFactor > 0 && !hasSealedReplica) {
            result.Status |= EChunkStatus::SealedMissing;
        }

        if (replicaCount < replicationFactor && hasSealedReplica) {
            result.Status |= EChunkStatus::Underreplicated;
        }

        if (totallySealed) {
            if (decommissionedReplicaCount > 0 && replicaCount + decommissionedReplicaCount > replicationFactor) {
                result.Status |= EChunkStatus::Overreplicated;
                if (inconsistentlyPlacedReplica) {
                    result.DecommissionedRemovalReplicas.push_back(inconsistentlyPlacedReplica);
                } else {
                    result.DecommissionedRemovalReplicas.insert(
                        result.DecommissionedRemovalReplicas.end(),
                        decommissionedReplicas.begin(),
                        decommissionedReplicas.end());
                }
            } else if (replicaCount > replicationFactor) {
                result.Status |= EChunkStatus::Overreplicated;
                if (inconsistentlyPlacedReplica) {
                    result.DecommissionedRemovalReplicas.push_back(inconsistentlyPlacedReplica);
                } else {
                    result.BalancingRemovalIndexes.push_back(GenericChunkReplicaIndex);
                }
            }
        }
    }

    if (replicationFactor > 1 && unsafelyPlacedReplica && None(result.Status & EChunkStatus::Overreplicated)) {
        result.Status |= EChunkStatus::UnsafelyPlaced;
        result.UnsafelyPlacedReplica = unsafelyPlacedReplica;
    }

    if (inconsistentlyPlacedReplica && None(result.Status & EChunkStatus::Overreplicated)) {
        result.Status |= EChunkStatus::InconsistentlyPlaced;
    }

    if (hasSealedReplica &&
        Any(result.Status & (EChunkStatus::Underreplicated | EChunkStatus::UnsafelyPlaced | EChunkStatus::InconsistentlyPlaced)))
    {
        result.ReplicationIndexes.push_back(GenericChunkReplicaIndex);
    }
}

void TChunkReplicator::ComputeRegularChunkStatisticsCrossMedia(
    TChunkStatistics& result,
    const TChunk* chunk,
    int totalReplicaCount,
    int totalDecommissionedReplicaCount,
    bool hasSealedReplicas,
    bool precarious,
    bool allMediaTransient,
    const TCompactVector<int, MaxMediumCount>& mediaOnWhichLost,
    bool hasMediumOnWhichPresent,
    bool hasMediumOnWhichUnderreplicated,
    bool hasMediumOnWhichSealedMissing)
{
    if (chunk->IsJournal() &&
        totalReplicaCount + totalDecommissionedReplicaCount < chunk->GetReadQuorum() &&
        !hasSealedReplicas)
    {
        result.Status |= ECrossMediumChunkStatus::QuorumMissing;
    }

    if (!hasMediumOnWhichPresent) {
        result.Status |= ECrossMediumChunkStatus::Lost;
    }

    if (precarious && !allMediaTransient) {
        result.Status |= ECrossMediumChunkStatus::Precarious;
    }

    if (!mediaOnWhichLost.empty() && hasMediumOnWhichPresent) {
        if (hasSealedReplicas) {
            for (auto mediumIndex : mediaOnWhichLost) {
                auto& mediumStatistics = result.PerMediumStatistics[mediumIndex];
                mediumStatistics.Status |= EChunkStatus::Underreplicated;
                mediumStatistics.ReplicationIndexes.push_back(GenericChunkReplicaIndex);
            }
        }
        result.Status |= ECrossMediumChunkStatus::MediumWiseLost;
    } else if (hasMediumOnWhichUnderreplicated || hasMediumOnWhichSealedMissing) {
        result.Status |= ECrossMediumChunkStatus::Deficient;
    }
}

void TChunkReplicator::OnNodeDisposed(TNode* node)
{
    YT_VERIFY(node->ChunkSealQueue().empty());
    YT_VERIFY(node->ChunkRemovalQueue().empty());
    for (const auto& queue : node->ChunkPushReplicationQueues()) {
        YT_VERIFY(queue.empty());
    }
}

void TChunkReplicator::OnNodeUnregistered(TNode* node)
{
    for (const auto& [chunkId, mediumToNode] : node->PushReplicationTargetNodeIds()) {
        for (auto [mediumIndex, nodeId] : mediumToNode) {
            MaybeRemoveFromPullReplicationSet(nodeId, chunkId, mediumIndex);
        }
    }

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    for (const auto& queue : node->ChunkPullReplicationQueues()) {
        for (const auto& [chunkReplica, mediumSet] : queue) {
            auto* chunk = chunkManager->FindChunk(chunkReplica.Id);
            if (IsObjectAlive(chunk)) {
                ScheduleChunkRefresh(chunk);
            }
        }
    }
}

void TChunkReplicator::OnChunkDestroyed(TChunk* chunk)
{
    // NB: We have to handle chunk here even if it should not be processed by replicator
    // since it may be in some of the queues being put when replicator processed this chunk.

    GetChunkRefreshScanner(chunk)->OnChunkDestroyed(chunk);
    GetChunkRequisitionUpdateScanner(chunk)->OnChunkDestroyed(chunk);
    ResetChunkStatus(chunk);
    RemoveChunkFromQueuesOnDestroy(chunk);
}

void TChunkReplicator::RemoveFromChunkReplicationQueues(
    TNode* node,
    TChunkPtrWithIndexes chunkWithIndexes)
{
    auto chunkId = chunkWithIndexes.GetPtr()->GetId();
    auto& pullReplicationNodeIds = node->PushReplicationTargetNodeIds();
    if (auto it = pullReplicationNodeIds.find(chunkId); it != pullReplicationNodeIds.end()) {
        for (auto [mediumIndex, nodeId] : it->second) {
            MaybeRemoveFromPullReplicationSet(nodeId, chunkId, mediumIndex);
        }
    }
    node->RemoveFromChunkReplicationQueues(chunkWithIndexes);
}

void TChunkReplicator::OnReplicaRemoved(
    TNode* node,
    TChunkPtrWithIndexes chunkWithIndexes,
    ERemoveReplicaReason reason)
{
    auto* chunk = chunkWithIndexes.GetPtr();

    // Fast path.
    if (!ShouldProcessChunk(chunk)) {
        return;
    }

    auto chunkIdWithIndexes = ToChunkIdWithIndexes(chunkWithIndexes);
    RemoveFromChunkReplicationQueues(node, chunkWithIndexes);
    if (reason != ERemoveReplicaReason::ChunkDestroyed) {
        node->RemoveFromChunkRemovalQueue(chunkIdWithIndexes);
    }
    if (chunk->IsJournal()) {
        node->RemoveFromChunkSealQueue(chunkWithIndexes);
    }
}

void TChunkReplicator::MaybeRemoveFromPullReplicationSet(
    TNodeId nodeId,
    TChunkId chunkId,
    int mediumIndex)
{
    if (!nodeId) {
        return;
    }

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    auto* node = nodeTracker->FindNode(nodeId);
    if (!node) {
        return;
    }

    node->RemoveFromPullReplicationSet(chunkId, mediumIndex);
}

bool TChunkReplicator::TryScheduleReplicationJob(
    IJobSchedulingContext* context,
    TChunkPtrWithIndexes chunkWithIndexes,
    TMedium* targetMedium,
    TNodeId targetNodeId)
{
    auto* sourceNode = context->GetNode();
    auto* chunk = chunkWithIndexes.GetPtr();
    auto replicaIndex = chunkWithIndexes.GetReplicaIndex();
    auto chunkId = chunk->GetId();
    auto mediumIndex = targetMedium->GetIndex();

    auto finallyGuard = Finally([&] {
        MaybeRemoveFromPullReplicationSet(targetNodeId, chunkId, mediumIndex);
    });

    if (!IsObjectAlive(chunk)) {
        return true;
    }

    if (chunk->GetScanFlag(EChunkScanKind::Refresh)) {
        return true;
    }

    if (chunk->HasJobs()) {
        return true;
    }

    int targetMediumIndex = targetMedium->GetIndex();
    const auto replicationFactor = GetChunkAggregatedReplicationFactor(chunk, targetMediumIndex);

    auto statistics = ComputeChunkStatistics(chunk);
    const auto& mediumStatistics = statistics.PerMediumStatistics[targetMediumIndex];
    int replicaCount = mediumStatistics.ReplicaCount[replicaIndex];

    if (Any(statistics.Status & ECrossMediumChunkStatus::Lost)) {
        return true;
    }

    if (replicaCount > replicationFactor) {
        return true;
    }

    int replicasNeeded;
    if (Any(mediumStatistics.Status & EChunkStatus::Underreplicated)) {
        replicasNeeded = replicationFactor - replicaCount;
    } else if (Any(mediumStatistics.Status & (EChunkStatus::UnsafelyPlaced | EChunkStatus::InconsistentlyPlaced))) {
        replicasNeeded = 1;
    } else {
        return true;
    }

    // TODO(babenko): journal replication currently does not support fan-out > 1
    if (chunk->IsJournal()) {
        replicasNeeded = 1;
    }

    auto targetNodes = ChunkPlacement_->AllocateWriteTargets(
        targetMedium,
        chunk,
        replicaIndex == GenericChunkReplicaIndex
            ? TChunkReplicaIndexList()
            : TChunkReplicaIndexList{replicaIndex},
        replicasNeeded,
        1,
        std::nullopt,
        ESessionType::Replication,
        mediumStatistics.UnsafelyPlacedReplica);

    if (targetNodes.empty()) {
        return false;
    }

    TNodePtrWithIndexesList targetReplicas;
    for (auto* node : targetNodes) {
        // TODO(aleksandra-zh): one of the nodes may still be overloaded, remove it from target list?
        targetReplicas.emplace_back(node, replicaIndex, targetMediumIndex);
    }

    auto job = New<TReplicationJob>(
        context->GenerateJobId(),
        GetJobEpoch(chunk),
        sourceNode,
        chunkWithIndexes,
        targetReplicas,
        targetNodeId);
    context->ScheduleJob(job);

    finallyGuard.Release();

    YT_LOG_DEBUG("Replication job scheduled "
        "(JobId: %v, JobEpoch: %v, Address: %v, ChunkId: %v, TargetAddresses: %v)",
        job->GetJobId(),
        job->GetJobEpoch(),
        sourceNode->GetDefaultAddress(),
        chunkWithIndexes,
        MakeFormattableView(targetNodes, TNodePtrAddressFormatter()));

    return std::ssize(targetNodes) == replicasNeeded;
}

bool TChunkReplicator::TryScheduleBalancingJob(
    IJobSchedulingContext* context,
    TChunkPtrWithIndexes chunkWithIndexes,
    double maxFillFactor)
{
    auto* sourceNode = context->GetNode();
    auto* chunk = chunkWithIndexes.GetPtr();

    if (chunk->GetScanFlag(EChunkScanKind::Refresh)) {
        return true;
    }

    if (chunk->HasJobs()) {
        return true;
    }

    auto replicaIndex = chunkWithIndexes.GetReplicaIndex();
    auto mediumIndex = chunkWithIndexes.GetMediumIndex();

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto* medium = chunkManager->GetMediumByIndex(mediumIndex);

    auto* targetNode = ChunkPlacement_->AllocateBalancingTarget(
        medium,
        chunk,
        maxFillFactor);
    if (!targetNode) {
        return false;
    }

    TNodePtrWithIndexesList targetReplicas{
        TNodePtrWithIndexes(targetNode, replicaIndex, mediumIndex)
    };

    auto job = New<TReplicationJob>(
        context->GenerateJobId(),
        GetJobEpoch(chunk),
        sourceNode,
        chunkWithIndexes,
        targetReplicas,
        InvalidNodeId);
    context->ScheduleJob(job);

    YT_LOG_DEBUG("Balancing job scheduled "
        "(JobId: %v, JobEpoch: %v, Address: %v, ChunkId: %v, TargetAddress: %v)",
        job->GetJobId(),
        job->GetJobEpoch(),
        sourceNode->GetDefaultAddress(),
        chunkWithIndexes,
        targetNode->GetDefaultAddress());

    return true;
}

bool TChunkReplicator::TryScheduleRemovalJob(
    IJobSchedulingContext* context,
    const TChunkIdWithIndexes& chunkIdWithIndexes)
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();

    auto* chunk = chunkManager->FindChunk(chunkIdWithIndexes.Id);
    // NB: Allow more than one job for dead chunks.
    if (IsObjectAlive(chunk)) {
        if (chunk->GetScanFlag(EChunkScanKind::Refresh)) {
            return true;
        }
        if (chunk->HasJobs()) {
            return true;
        }
    }

    auto shardIndex = GetChunkShardIndex(chunkIdWithIndexes.Id);
    auto job = New<TRemovalJob>(
        context->GenerateJobId(),
        JobEpochs_[shardIndex],
        context->GetNode(),
        IsObjectAlive(chunk) ? chunk : nullptr,
        chunkIdWithIndexes);
    context->ScheduleJob(job);

    YT_LOG_DEBUG("Removal job scheduled "
        "(JobId: %v, JobEpoch: %v, Address: %v, ChunkId: %v)",
        job->GetJobId(),
        job->GetJobEpoch(),
        context->GetNode()->GetDefaultAddress(),
        chunkIdWithIndexes);

    return true;
}

bool TChunkReplicator::TryScheduleRepairJob(
    IJobSchedulingContext* context,
    EChunkRepairQueue repairQueue,
    TChunkPtrWithIndexes chunkWithIndexes)
{
    YT_VERIFY(chunkWithIndexes.GetReplicaIndex() == GenericChunkReplicaIndex);

    auto* chunk = chunkWithIndexes.GetPtr();
    int mediumIndex = chunkWithIndexes.GetMediumIndex();

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto* medium = chunkManager->GetMediumByIndex(mediumIndex);

    YT_VERIFY(chunk->IsErasure());

    if (!IsObjectAlive(chunk)) {
        return true;
    }

    if (chunk->GetScanFlag(EChunkScanKind::Refresh)) {
        return true;
    }

    if (chunk->HasJobs()) {
        return true;
    }

    auto codecId = chunk->GetErasureCodec();
    auto* codec = NErasure::GetCodec(codecId);
    auto totalPartCount = codec->GetTotalPartCount();

    auto statistics = ComputeChunkStatistics(chunk);
    const auto& mediumStatistics = statistics.PerMediumStatistics[mediumIndex];

    NErasure::TPartIndexList erasedPartIndexes;
    for (int index = 0; index < totalPartCount; ++index) {
        if (mediumStatistics.ReplicaCount[index] == 0) {
            erasedPartIndexes.push_back(index);
        }
    }

    if (erasedPartIndexes.empty()) {
        return true;
    }

    if (!codec->CanRepair(erasedPartIndexes)) {
        // Can't repair without decommissioned replicas. Use them.
        auto guaranteedRepairablePartCount = codec->GetGuaranteedRepairablePartCount();
        YT_VERIFY(guaranteedRepairablePartCount < static_cast<int>(erasedPartIndexes.size()));

        // Reorder the parts so that the actually erased ones go first and then the decommissioned ones.
        std::partition(
            erasedPartIndexes.begin(),
            erasedPartIndexes.end(),
            [&] (int index) {
                return mediumStatistics.DecommissionedReplicaCount[index] == 0;
            });

        // Try popping decommissioned replicas as long repair cannot be performed.
        do {
            if (mediumStatistics.DecommissionedReplicaCount[erasedPartIndexes.back()] == 0) {
                YT_LOG_ERROR("Erasure chunk has not enough replicas to repair (ChunkId: %v)",
                    chunk->GetId());
                return false;
            }
            erasedPartIndexes.pop_back();
        } while (!codec->CanRepair(erasedPartIndexes));

        std::sort(erasedPartIndexes.begin(), erasedPartIndexes.end());
    }

    auto targetNodes = ChunkPlacement_->AllocateWriteTargets(
        medium,
        chunk,
        {erasedPartIndexes.begin(), erasedPartIndexes.end()},
        std::ssize(erasedPartIndexes),
        std::ssize(erasedPartIndexes),
        std::nullopt,
        ESessionType::Repair);

    if (targetNodes.empty()) {
        return false;
    }

    YT_VERIFY(std::ssize(targetNodes) == std::ssize(erasedPartIndexes));

    TNodePtrWithIndexesList targetReplicas;
    int targetIndex = 0;
    for (auto* node : targetNodes) {
        targetReplicas.emplace_back(node, erasedPartIndexes[targetIndex++], mediumIndex);
    }

    auto job = New<TRepairJob>(
        context->GenerateJobId(),
        GetJobEpoch(chunk),
        context->GetNode(),
        GetDynamicConfig()->RepairJobMemoryUsage,
        chunk,
        targetReplicas,
        repairQueue == EChunkRepairQueue::Decommissioned);
    context->ScheduleJob(job);

    ChunkRepairQueueBalancer(repairQueue).AddWeight(
        mediumIndex,
        job->ResourceUsage().repair_data_size() * job->TargetReplicas().size());

    YT_LOG_DEBUG("Repair job scheduled "
        "(JobId: %v, JobEpoch: %v, Address: %v, ChunkId: %v, Targets: %v, ErasedPartIndexes: %v)",
        job->GetJobId(),
        job->GetJobEpoch(),
        context->GetNode()->GetDefaultAddress(),
        chunkWithIndexes,
        MakeFormattableView(targetNodes, TNodePtrAddressFormatter()),
        erasedPartIndexes);

    return true;
}

void TChunkReplicator::ScheduleJobs(IJobSchedulingContext* context)
{
    if (!IsReplicatorEnabled()) {
        return;
    }

    auto* node = context->GetNode();
    const auto& resourceUsage = context->GetNodeResourceUsage();
    const auto& resourceLimits = context->GetNodeResourceLimits();

    int misscheduledReplicationJobs = 0;
    int misscheduledRepairJobs = 0;
    int misscheduledRemovalJobs = 0;

    // NB: Beware of chunks larger than the limit; we still need to be able to replicate them one by one.
    auto hasSpareReplicationResources = [&] {
        return
            misscheduledReplicationJobs < GetDynamicConfig()->MaxMisscheduledReplicationJobsPerHeartbeat &&
            resourceUsage.replication_slots() < resourceLimits.replication_slots() &&
            (resourceUsage.replication_slots() == 0 || resourceUsage.replication_data_size() < resourceLimits.replication_data_size());
    };

    // NB: Beware of chunks larger than the limit; we still need to be able to repair them one by one.
    auto hasSpareRepairResources = [&] {
        return
            misscheduledRepairJobs < GetDynamicConfig()->MaxMisscheduledRepairJobsPerHeartbeat &&
            resourceUsage.repair_slots() < resourceLimits.repair_slots() &&
            (resourceUsage.repair_slots() == 0 || resourceUsage.repair_data_size() < resourceLimits.repair_data_size());
    };

    auto hasSpareRemovalResources = [&] {
        return
            misscheduledRemovalJobs < GetDynamicConfig()->MaxMisscheduledRemovalJobsPerHeartbeat &&
            resourceUsage.removal_slots() < resourceLimits.removal_slots();
    };

    auto hasSparePullReplicationResources = [&] {
        return
            // TODO(gritukan): Use some better bounds.
            misscheduledReplicationJobs < GetDynamicConfig()->MaxMisscheduledReplicationJobsPerHeartbeat &&
            std::ssize(node->ChunksBeingPulled()) < GetDynamicConfig()->MaxRunningReplicationJobsPerTargetNode;
    };

    const auto& chunkManager = Bootstrap_->GetChunkManager();

    // Move CRP-enabled chunks from pull to push queues.
    auto& queues = node->ChunkPullReplicationQueues();
    for (int priority = 0; priority < std::ssize(queues); ++priority) {
        auto& queue = queues[priority];
        auto it = queue.begin();
        while (it != queue.end() && hasSparePullReplicationResources()) {
            auto jt = it++;
            auto desiredReplica = jt->first;
            auto chunkId = desiredReplica.Id;
            auto* chunk = chunkManager->FindChunk(chunkId);
            auto& mediumIndexSet = jt->second;

            if (!IsObjectAlive(chunk) || !chunk->IsRefreshActual()) {
                queue.erase(jt);
                ++misscheduledReplicationJobs;
                continue;
            }

            for (const auto& replica : chunk->StoredReplicas()) {
                auto* pushNode = replica.GetPtr();
                for (int mediumIndex = 0; mediumIndex < std::ssize(mediumIndexSet); ++mediumIndex) {
                    if (mediumIndexSet.test(mediumIndex)) {
                        if (auto nodeId = pushNode->GetTargetReplicationNodeId(chunk->GetId(), mediumIndex)) {
                            // Replication is already planned with another node as a destination.
                            continue;
                        }

                        if (desiredReplica.ReplicaIndex != replica.GetReplicaIndex()) {
                            continue;
                        }

                        TChunkPtrWithIndexes chunkWithIndexes(chunk, replica.GetReplicaIndex(), replica.GetMediumIndex());
                        pushNode->AddToChunkPushReplicationQueue(chunkWithIndexes, mediumIndex, priority);
                        pushNode->AddTargetReplicationNodeId(chunk->GetId(), mediumIndex, node);

                        node->AddToPullReplicationSet(chunk->GetId(), mediumIndex);
                    }
                }
            }

            queue.erase(jt);
        }
    }

    // Schedule replication jobs.
    for (auto& queue : node->ChunkPushReplicationQueues()) {
        auto it = queue.begin();
        while (it != queue.end() && hasSpareReplicationResources()) {
            auto jt = it++;
            auto chunkWithIndexes = jt->first;
            auto* chunk = chunkWithIndexes.GetPtr();
            auto& mediumIndexSet = jt->second;

            if (!chunk->IsRefreshActual()) {
                queue.erase(jt);
                node->RemoveTargetReplicationNodeId(chunk->GetId());
                ++misscheduledReplicationJobs;
                continue;
            }

            for (int mediumIndex = 0; mediumIndex < std::ssize(mediumIndexSet); ++mediumIndex) {
                if (mediumIndexSet.test(mediumIndex)) {
                    auto* medium = chunkManager->GetMediumByIndex(mediumIndex);
                    auto nodeId = node->GetTargetReplicationNodeId(chunk->GetId(), mediumIndex);
                    node->RemoveTargetReplicationNodeId(chunk->GetId(), mediumIndex);

                    if (TryScheduleReplicationJob(context, chunkWithIndexes, medium, nodeId)) {
                        mediumIndexSet.reset(mediumIndex);
                    } else {
                        ++misscheduledReplicationJobs;
                    }
                }
            }

            if (mediumIndexSet.none()) {
                queue.erase(jt);
            }
        }
    }

    // Schedule repair jobs.
    // NB: the order of the enum items is crucial! Part-missing chunks must
    // be repaired before part-decommissioned chunks.
    for (auto queue : TEnumTraits<EChunkRepairQueue>::GetDomainValues()) {
        TMediumMap<std::pair<TChunkRepairQueue::iterator, TChunkRepairQueue::iterator>> iteratorPerRepairQueue;
        for (int mediumIndex = 0; mediumIndex < MaxMediumCount; ++mediumIndex) {
            auto& chunkRepairQueue = ChunkRepairQueue(mediumIndex, queue);
            if (!chunkRepairQueue.empty()) {
                iteratorPerRepairQueue[mediumIndex] = std::make_pair(chunkRepairQueue.begin(), chunkRepairQueue.end());
            }
        }

        while (hasSpareRepairResources()) {
            auto winner = ChunkRepairQueueBalancer(queue).TakeWinnerIf(
                [&] (int mediumIndex) {
                    // Don't repair chunks on nodes without relevant medium.
                    // In particular, this avoids repairing non-cloud tables in the cloud.
                    const auto it = iteratorPerRepairQueue.find(mediumIndex);
                    return node->HasMedium(mediumIndex)
                        && it != iteratorPerRepairQueue.end()
                        && it->second.first != it->second.second;
                });

            if (!winner) {
                break; // Nothing to repair on relevant media.
            }

            auto mediumIndex = *winner;
            auto& chunkRepairQueue = ChunkRepairQueue(mediumIndex, queue);
            auto chunkIt = iteratorPerRepairQueue[mediumIndex].first++;
            auto chunkWithIndexes = *chunkIt;
            auto* chunk = chunkWithIndexes.GetPtr();

            auto removeFromQueue = [&] {
                chunk->SetRepairQueueIterator(chunkWithIndexes.GetMediumIndex(), queue, TChunkRepairQueueIterator());
                chunkRepairQueue.erase(chunkIt);
            };

            // NB: Repair queues are not cleared when shard processing is stopped,
            // so we have to handle chunks replicator should not process.
            if (!chunk->IsRefreshActual()) {
                removeFromQueue();
                ++misscheduledRepairJobs;
            } else if (TryScheduleRepairJob(context, queue, chunkWithIndexes)) {
                removeFromQueue();
            } else {
                ++misscheduledRepairJobs;
            }
        }
    }

    // Schedule removal jobs.
    const auto& nodeJobs = JobRegistry_->GetNodeJobs(node->GetDefaultAddress());

    THashSet<TChunkIdWithIndexes> chunksBeingRemoved;
    for (const auto& job : nodeJobs) {
        if (job->GetType() != EJobType::RemoveChunk) {
            continue;
        }
        chunksBeingRemoved.insert(job->GetChunkIdWithIndexes());
    }
    {
        const auto& queue = node->DestroyedReplicas();
        auto it = node->GetDestroyedReplicasIterator();
        auto jt = it;
        do {
            if (queue.empty() || !hasSpareRemovalResources()) {
                break;
            }

            if (!chunksBeingRemoved.contains(*jt)) {
                if (TryScheduleRemovalJob(context, *jt)) {
                    node->AdvanceDestroyedReplicasIterator();
                } else {
                    ++misscheduledRemovalJobs;
                }
            }

            ++jt;
            if (jt == queue.end()) {
                jt = queue.begin();
            }
        } while (jt != it);
    }
    {
        auto& queue = node->ChunkRemovalQueue();
        auto it = queue.begin();
        while (it != queue.end()) {
            if (!hasSpareRemovalResources()) {
                break;
            }

            auto jt = it++;
            auto chunkIdWithIndex = jt->first;
            auto& mediumIndexSet = jt->second;

            auto* chunk = chunkManager->FindChunk(chunkIdWithIndex.Id);
            if (IsObjectAlive(chunk) && !chunk->IsRefreshActual()) {
                queue.erase(jt);
                ++misscheduledRemovalJobs;
                continue;
            }

            for (int mediumIndex = 0; mediumIndex < std::ssize(mediumIndexSet); ++mediumIndex) {
                if (mediumIndexSet.test(mediumIndex)) {
                    TChunkIdWithIndexes chunkIdWithIndexes(
                        chunkIdWithIndex.Id,
                        chunkIdWithIndex.ReplicaIndex,
                        mediumIndex);

                    if (chunksBeingRemoved.contains(chunkIdWithIndexes)) {
                        YT_LOG_ALERT(
                            "Trying to schedule a removal job for a chunk that is already being removed (ChunkId: %v)",
                            chunkIdWithIndexes);
                        mediumIndexSet.reset(mediumIndex);
                        continue;
                    }
                    if (TryScheduleRemovalJob(context, chunkIdWithIndexes)) {
                        mediumIndexSet.reset(mediumIndex);
                    } else {
                        ++misscheduledRemovalJobs;
                    }
                }
            }
            if (mediumIndexSet.none()) {
                queue.erase(jt);
            }
        }
    }

    // Schedule balancing jobs.
    // TODO(gritukan): YT-17358, balancing jobs are probably not needed and are hard to
    // implement with sharded replicator.
    if (RefreshRunning_.all()) {
        for (const auto& mediumIdAndPtrPair : Bootstrap_->GetChunkManager()->Media()) {
            auto* medium = mediumIdAndPtrPair.second;
            if (medium->GetCache()) {
                continue;
            }

            auto mediumIndex = medium->GetIndex();
            auto sourceFillFactor = node->GetFillFactor(mediumIndex);
            if (!sourceFillFactor) {
                continue; // No storage of this medium on this node.
            }

            double targetFillFactor = *sourceFillFactor - GetDynamicConfig()->MinChunkBalancingFillFactorDiff;
            if (hasSpareReplicationResources() &&
                *sourceFillFactor > GetDynamicConfig()->MinChunkBalancingFillFactor &&
                ChunkPlacement_->HasBalancingTargets(
                    medium,
                    targetFillFactor))
            {
                int maxJobs = std::max(0, resourceLimits.replication_slots() - resourceUsage.replication_slots());
                auto chunksToBalance = ChunkPlacement_->GetBalancingChunks(medium, node, maxJobs);
                for (auto chunkWithIndexes : chunksToBalance) {
                    if (!hasSpareReplicationResources()) {
                        break;
                    }

                    if (!TryScheduleBalancingJob(context, chunkWithIndexes, targetFillFactor)) {
                        ++misscheduledReplicationJobs;
                    }
                }
            }
        }
    }

    MisscheduledJobs_[EJobType::ReplicateChunk] += misscheduledReplicationJobs;
    MisscheduledJobs_[EJobType::RepairChunk] += misscheduledRepairJobs;
    MisscheduledJobs_[EJobType::RemoveChunk] += misscheduledRemovalJobs;
}

void TChunkReplicator::RefreshChunk(TChunk* chunk)
{
    if (!chunk->IsConfirmed()) {
        return;
    }

    if (chunk->IsForeign()) {
        return;
    }

    chunk->OnRefresh();

    const auto replication = GetChunkAggregatedReplication(chunk);

    ResetChunkStatus(chunk);
    RemoveChunkFromQueuesOnRefresh(chunk);

    auto allMediaStatistics = ComputeChunkStatistics(chunk);

    auto durabilityRequired = false;

    const auto& chunkManager = Bootstrap_->GetChunkManager();

    for (auto entry : replication) {
        auto mediumIndex = entry.GetMediumIndex();
        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        YT_VERIFY(IsObjectAlive(medium));

        // For now, chunk cache-as-medium support is rudimentary, and replicator
        // ignores chunk cache to preserve original behavior.
        if (medium->GetCache()) {
            continue;
        }

        auto& statistics = allMediaStatistics.PerMediumStatistics[mediumIndex];
        if (statistics.Status == EChunkStatus::None) {
            continue;
        }

        auto replicationFactor = entry.Policy().GetReplicationFactor();
        auto durabilityRequiredOnMedium =
            replication.GetVital() &&
            (chunk->IsErasure() || replicationFactor > 1) &&
            !medium->GetTransient();
        durabilityRequired = durabilityRequired || durabilityRequiredOnMedium;

        if (Any(statistics.Status & EChunkStatus::Overreplicated)) {
            OverreplicatedChunks_.insert(chunk);
        }

        if (Any(statistics.Status & EChunkStatus::Underreplicated)) {
            UnderreplicatedChunks_.insert(chunk);
        }

        if (Any(statistics.Status & EChunkStatus::UnsafelyPlaced)) {
            UnsafelyPlacedChunks_.insert(chunk);
        }

        if (Any(statistics.Status & EChunkStatus::InconsistentlyPlaced)) {
            InconsistentlyPlacedChunks_.insert(chunk);
        }

        if (!chunk->HasJobs()) {
            if (Any(statistics.Status & EChunkStatus::Overreplicated) &&
                None(allMediaStatistics.Status & (ECrossMediumChunkStatus::Deficient | ECrossMediumChunkStatus::MediumWiseLost)))
            {
                for (auto nodeWithIndexes : statistics.DecommissionedRemovalReplicas) {
                    auto* node = nodeWithIndexes.GetPtr();
                    if (!node->ReportedDataNodeHeartbeat()) {
                        continue;
                    }

                    YT_ASSERT(mediumIndex == nodeWithIndexes.GetMediumIndex());
                    TChunkIdWithIndexes chunkIdWithIndexes(chunk->GetId(), nodeWithIndexes.GetReplicaIndex(), nodeWithIndexes.GetMediumIndex());
                    node->AddToChunkRemovalQueue(chunkIdWithIndexes);
                }

                for (int replicaIndex : statistics.BalancingRemovalIndexes) {
                    TChunkPtrWithIndexes chunkWithIndexes(chunk, replicaIndex, mediumIndex);
                    auto* targetNode = ChunkPlacement_->GetRemovalTarget(chunkWithIndexes);
                    if (!targetNode) {
                        continue;
                    }

                    TChunkIdWithIndexes chunkIdWithIndexes(chunk->GetId(), replicaIndex, mediumIndex);
                    targetNode->AddToChunkRemovalQueue(chunkIdWithIndexes);
                }
            }

            // This check may yield true even for lost chunks when cross-medium replication is in progress.
            if (Any(statistics.Status & (EChunkStatus::Underreplicated | EChunkStatus::UnsafelyPlaced | EChunkStatus::InconsistentlyPlaced))) {
                for (auto replicaIndex : statistics.ReplicationIndexes) {
                    // Cap replica count minus one against the range [0, ReplicationPriorityCount - 1].
                    int replicaCount = statistics.ReplicaCount[replicaIndex];
                    int priority = std::max(std::min(replicaCount - 1, ReplicationPriorityCount - 1), 0);

                    if (UsePullReplication(chunk)) {
                        for (auto replica : statistics.MissingReplicas) {
                            auto* node = replica.GetPtr();
                            if (!node->ReportedDataNodeHeartbeat()) {
                                YT_LOG_ALERT("Trying to place a chunk on a node that did not report a heartbeat (ChunkId: %v, NodeId: %v)",
                                    chunk->GetId(),
                                    node->GetId());
                                continue;
                            }

                            TChunkPtrWithIndexes chunkWithIndexes(chunk, replica.GetReplicaIndex(), replica.GetMediumIndex());
                            node->AddToChunkPullReplicationQueue(chunkWithIndexes, mediumIndex, priority);
                        }
                    } else {
                        for (auto replica : chunk->StoredReplicas()) {
                            if (chunk->IsJournal() && replica.GetState() != EChunkReplicaState::Sealed) {
                                continue;
                            }

                            if (replica.GetReplicaIndex() != replicaIndex) {
                                continue;
                            }

                            // If chunk is lost on some media, don't match dst medium with
                            // src medium: we want to be able to do cross-medium replication.
                            bool mediumMatches =
                                Any(allMediaStatistics.Status & ECrossMediumChunkStatus::MediumWiseLost) ||
                                mediumIndex == replica.GetMediumIndex();
                            if (!mediumMatches) {
                                continue;
                            }

                            auto* node = replica.GetPtr();
                            if (!node->ReportedDataNodeHeartbeat()) {
                                continue;
                            }

                            TChunkPtrWithIndexes chunkWithIndexes(chunk, replica.GetReplicaIndex(), replica.GetMediumIndex());
                            node->AddToChunkPushReplicationQueue(chunkWithIndexes, mediumIndex, priority);
                        }
                    }
                }
            }

            if (None(statistics.Status & EChunkStatus::Lost) && chunk->IsSealed()) {
                TChunkPtrWithIndexes chunkWithIndexes(chunk, GenericChunkReplicaIndex, mediumIndex);
                if (Any(statistics.Status & (EChunkStatus::DataMissing | EChunkStatus::ParityMissing))) {
                    AddToChunkRepairQueue(chunkWithIndexes, EChunkRepairQueue::Missing);
                } else if (Any(statistics.Status & (EChunkStatus::DataDecommissioned | EChunkStatus::ParityDecommissioned))) {
                    AddToChunkRepairQueue(chunkWithIndexes, EChunkRepairQueue::Decommissioned);
                }
            }
        }
    }

    if (Any(allMediaStatistics.Status & ECrossMediumChunkStatus::Sealed)) {
        YT_ASSERT(chunk->IsJournal());
        for (auto replica : chunk->StoredReplicas()) {
            if (replica.GetState() != EChunkReplicaState::Unsealed) {
                continue;
            }

            auto* node = replica.GetPtr();
            if (!node->ReportedDataNodeHeartbeat()) {
                continue;
            }

            TChunkPtrWithIndexes chunkWithIndexes(chunk, replica.GetReplicaIndex(), replica.GetMediumIndex());
            node->AddToChunkSealQueue(chunkWithIndexes);
        }
    }

    if (Any(allMediaStatistics.Status & ECrossMediumChunkStatus::Lost)) {
        YT_VERIFY(LostChunks_.insert(chunk).second);
        if (durabilityRequired) {
            YT_VERIFY(LostVitalChunks_.insert(chunk).second);
        }
    }

    if (Any(allMediaStatistics.Status & ECrossMediumChunkStatus::DataMissing)) {
        YT_ASSERT(chunk->IsErasure());
        YT_VERIFY(DataMissingChunks_.insert(chunk).second);
    }

    if (Any(allMediaStatistics.Status & ECrossMediumChunkStatus::ParityMissing)) {
        YT_ASSERT(chunk->IsErasure());
        YT_VERIFY(ParityMissingChunks_.insert(chunk).second);
    }

    if (Any(allMediaStatistics.Status & ECrossMediumChunkStatus::QuorumMissing)) {
        YT_ASSERT(chunk->IsJournal());
        YT_VERIFY(QuorumMissingChunks_.insert(chunk).second);
    }

    if (Any(allMediaStatistics.Status & ECrossMediumChunkStatus::Precarious)) {
        YT_VERIFY(PrecariousChunks_.insert(chunk).second);
        if (durabilityRequired) {
            YT_VERIFY(PrecariousVitalChunks_.insert(chunk).second);
        }
    }

    if (Any(allMediaStatistics.Status & (ECrossMediumChunkStatus::DataMissing | ECrossMediumChunkStatus::ParityMissing))) {
        if (!chunk->GetPartLossTime()) {
            chunk->SetPartLossTime(GetCpuInstant());
        }
        MaybeRememberPartMissingChunk(chunk);
    } else if (chunk->GetPartLossTime()) {
        chunk->ResetPartLossTime();
    }

    if (chunk->IsBlob() && chunk->GetEndorsementRequired()) {
        ChunkIdsPendingEndorsementRegistration_.push_back(chunk->GetId());
    }
}

void TChunkReplicator::ResetChunkStatus(TChunk* chunk)
{
    LostChunks_.erase(chunk);
    LostVitalChunks_.erase(chunk);
    PrecariousChunks_.erase(chunk);
    PrecariousVitalChunks_.erase(chunk);

    UnderreplicatedChunks_.erase(chunk);
    OverreplicatedChunks_.erase(chunk);
    UnsafelyPlacedChunks_.erase(chunk);
    if (chunk->HasConsistentReplicaPlacementHash()) {
        InconsistentlyPlacedChunks_.erase(chunk);
    }

    if (chunk->IsErasure()) {
        DataMissingChunks_.erase(chunk);
        ParityMissingChunks_.erase(chunk);
        OldestPartMissingChunks_.erase(chunk);
    }

    if (chunk->IsJournal()) {
        QuorumMissingChunks_.erase(chunk);
    }
}

void TChunkReplicator::MaybeRememberPartMissingChunk(TChunk* chunk)
{
    YT_ASSERT(chunk->GetPartLossTime());

    // A chunk from an earlier epoch couldn't have made it to OldestPartMissingChunks_.
    YT_VERIFY(OldestPartMissingChunks_.empty() || (*OldestPartMissingChunks_.begin())->GetPartLossTime());

    if (std::ssize(OldestPartMissingChunks_) >= GetDynamicConfig()->MaxOldestPartMissingChunks) {
        return;
    }

    if (OldestPartMissingChunks_.empty()) {
        OldestPartMissingChunks_.insert(chunk);
        return;
    }

    auto* mostRecentPartMissingChunk = *OldestPartMissingChunks_.rbegin();
    auto mostRecentPartLossTime = mostRecentPartMissingChunk->GetPartLossTime();

    if (chunk->GetPartLossTime() >= mostRecentPartLossTime) {
        return;
    }

    OldestPartMissingChunks_.erase(--OldestPartMissingChunks_.end());
    OldestPartMissingChunks_.insert(chunk);
}

void TChunkReplicator::RemoveChunkFromQueuesOnRefresh(TChunk* chunk)
{
    for (auto replica : chunk->StoredReplicas()) {
        auto* node = replica.GetPtr();

        // Remove from replication queue.
        TChunkPtrWithIndexes chunkWithIndexes(chunk, replica.GetReplicaIndex(), replica.GetMediumIndex());
        RemoveFromChunkReplicationQueues(node, chunkWithIndexes);

        // Remove from removal queue.
        TChunkIdWithIndexes chunkIdWithIndexes(chunk->GetId(), replica.GetReplicaIndex(), replica.GetMediumIndex());
        node->RemoveFromChunkRemovalQueue(chunkIdWithIndexes);
    }

    const auto& requisition = chunk->GetAggregatedRequisition(GetChunkRequisitionRegistry());
    for (const auto& entry : requisition) {
        const auto& mediumIndex = entry.MediumIndex;
        const auto* medium = Bootstrap_->GetChunkManager()->FindMediumByIndex(mediumIndex);
        if (medium->GetCache()) {
            continue;
        }

        // Remove from repair queue.
        TChunkPtrWithIndexes chunkWithIndexes(chunk, GenericChunkReplicaIndex, mediumIndex);
        RemoveFromChunkRepairQueues(chunkWithIndexes);
    }
}

void TChunkReplicator::RemoveChunkFromQueuesOnDestroy(TChunk* chunk)
{
    // Remove chunk from replication and seal queues.
    for (auto replica : chunk->StoredReplicas()) {
        auto* node = replica.GetPtr();
        TChunkPtrWithIndexes chunkWithIndexes(chunk, replica.GetReplicaIndex(), replica.GetMediumIndex());
        // NB: Keep existing removal requests to workaround the following scenario:
        // 1) the last strong reference to a chunk is released while some ephemeral references
        //    remain; the chunk becomes a zombie;
        // 2) a node sends a heartbeat reporting addition of the chunk;
        // 3) master receives the heartbeat and puts the chunk into the removal queue
        //    without (sic!) registering a replica;
        // 4) the last ephemeral reference is dropped, the chunk is being removed;
        //    at this point we must preserve its removal request in the queue.
        RemoveFromChunkReplicationQueues(node, chunkWithIndexes);
        node->RemoveFromChunkSealQueue(chunkWithIndexes);
    }

    // Remove chunk from repair queues.
    if (chunk->IsErasure()) {
        const auto& requisition = chunk->GetAggregatedRequisition(GetChunkRequisitionRegistry());
        for (const auto& entry : requisition) {
            auto mediumIndex = entry.MediumIndex;
            TChunkPtrWithIndexes chunkPtrWithIndexes(chunk, GenericChunkReplicaIndex, mediumIndex);
            RemoveFromChunkRepairQueues(chunkPtrWithIndexes);
        }
    }
}

bool TChunkReplicator::IsReplicaDecommissioned(TNodePtrWithIndexes replica)
{
    auto* node = replica.GetPtr();
    return node->GetDecommissioned();
}

TChunkReplication TChunkReplicator::GetChunkAggregatedReplication(const TChunk* chunk)
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto result = chunk->GetAggregatedReplication(GetChunkRequisitionRegistry());
    for (auto& entry : result) {
        YT_VERIFY(entry.Policy());

        auto* medium = chunkManager->FindMediumByIndex(entry.GetMediumIndex());
        YT_VERIFY(IsObjectAlive(medium));
        auto cap = medium->Config()->MaxReplicationFactor;

        entry.Policy().SetReplicationFactor(std::min(cap, entry.Policy().GetReplicationFactor()));
    }

    // A chunk may happen to have replicas stored on a medium it's not supposed
    // to have replicas on. (This is common when chunks are being relocated from
    // one medium to another.) Add corresponding entries to the aggregated
    // replication so that such media aren't overlooked.
    for (auto replica : chunk->StoredReplicas()) {
        auto mediumIndex = replica.GetMediumIndex();
        if (!result.Contains(mediumIndex)) {
            result.Set(mediumIndex, TReplicationPolicy(), false /*eraseEmpty*/);
        }
    }

    return result;
}

int TChunkReplicator::GetChunkAggregatedReplicationFactor(const TChunk* chunk, int mediumIndex)
{
    auto result = chunk->GetAggregatedReplicationFactor(mediumIndex, GetChunkRequisitionRegistry());

    auto* medium = Bootstrap_->GetChunkManager()->FindMediumByIndex(mediumIndex);
    YT_VERIFY(IsObjectAlive(medium));
    auto cap = medium->Config()->MaxReplicationFactor;

    return std::min(cap, result);
}

void TChunkReplicator::ScheduleChunkRefresh(TChunk* chunk)
{
    // Fast path.
    if (!ShouldProcessChunk(chunk)) {
        return;
    }

    if (!IsObjectAlive(chunk)) {
        return;
    }

    if (chunk->IsForeign()) {
        return;
    }

    GetChunkRefreshScanner(chunk)->EnqueueChunk(chunk);
}

void TChunkReplicator::ScheduleNodeRefresh(TNode* node)
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();

    for (const auto& [mediumIndex, replicas] : node->Replicas()) {
        const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        if (!medium || medium->GetCache()) {
            continue;
        }

        for (auto replica : replicas) {
            ScheduleChunkRefresh(replica.GetPtr());
        }
    }
}

void TChunkReplicator::ScheduleGlobalChunkRefresh()
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
        if (RefreshRunning_.test(shardIndex)) {
            BlobRefreshScanner_->ScheduleGlobalScan(shardIndex, chunkManager->GetGlobalBlobChunkScanDescriptor(shardIndex));
            JournalRefreshScanner_->ScheduleGlobalScan(shardIndex, chunkManager->GetGlobalJournalChunkScanDescriptor(shardIndex));
        }
    }
}

void TChunkReplicator::OnRefresh()
{
    if (!GetDynamicConfig()->EnableChunkRefresh) {
        YT_LOG_DEBUG("Chunk refresh disabled");
        return;
    }

    YT_LOG_DEBUG("Chunk refresh iteration started");

    auto deadline = GetCpuInstant() - DurationToCpuDuration(GetDynamicConfig()->ChunkRefreshDelay);

    auto doRefreshChunks = [&] (
        const std::unique_ptr<TChunkScanner>& scanner,
        int* const totalCount,
        int* const aliveCount,
        int maxChunksPerRefresh,
        TDuration maxTimePerRefresh)
    {
        NProfiling::TWallTimer timer;

        while (*totalCount < maxChunksPerRefresh && scanner->HasUnscannedChunk(deadline)) {
            if (timer.GetElapsedTime() > maxTimePerRefresh) {
                break;
            }

            ++(*totalCount);
            auto* chunk = scanner->DequeueChunk();
            if (!chunk) {
                continue;
            }

            RefreshChunk(chunk);
            ++(*aliveCount);
        }
    };

    int totalBlobCount = 0;
    int totalJournalCount = 0;
    int aliveBlobCount = 0;
    int aliveJournalCount = 0;

    ChunkIdsPendingEndorsementRegistration_.clear();

    YT_PROFILE_TIMING("/chunk_server/refresh_time") {
        doRefreshChunks(
            BlobRefreshScanner_,
            &totalBlobCount,
            &aliveBlobCount,
            GetDynamicConfig()->MaxBlobChunksPerRefresh,
            GetDynamicConfig()->MaxTimePerBlobChunkRefresh);
        doRefreshChunks(
            JournalRefreshScanner_,
            &totalJournalCount,
            &aliveJournalCount,
            GetDynamicConfig()->MaxJournalChunksPerRefresh,
            GetDynamicConfig()->MaxTimePerJournalChunkRefresh);
    }

    FlushEndorsementQueue();

    YT_LOG_DEBUG("Chunk refresh iteration completed (TotalBlobCount: %v, AliveBlobCount: %v, TotalJournalCount: %v, AliveJournalCount: %v)",
        totalBlobCount,
        aliveBlobCount,
        totalJournalCount,
        aliveJournalCount);
}

bool TChunkReplicator::IsReplicatorEnabled()
{
    return Enabled_.value_or(false);
}

bool TChunkReplicator::IsRefreshEnabled()
{
    return GetDynamicConfig()->EnableChunkRefresh;
}

bool TChunkReplicator::IsRequisitionUpdateEnabled()
{
    return GetDynamicConfig()->EnableChunkRequisitionUpdate;
}

bool TChunkReplicator::ShouldProcessChunk(TChunk* chunk)
{
    return RefreshRunning_.test(chunk->GetShardIndex());
}

TJobEpoch TChunkReplicator::GetJobEpoch(TChunk* chunk) const
{
    return JobEpochs_[chunk->GetShardIndex()];
}

void TChunkReplicator::OnCheckEnabled()
{
    const auto& worldInitializer = Bootstrap_->GetWorldInitializer();
    if (!worldInitializer->IsInitialized()) {
        return;
    }

    try {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            OnCheckEnabledPrimary();
        } else {
            OnCheckEnabledSecondary();
        }
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Error updating chunk replicator state, disabling until the next attempt");
        Enabled_ = false;
    }
}

void TChunkReplicator::OnCheckEnabledPrimary()
{
    if (!GetDynamicConfig()->EnableChunkReplicator) {
        if (!Enabled_ || *Enabled_) {
            YT_LOG_INFO("Chunk replicator disabled");
        }
        Enabled_ = false;
        return;
    }

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    int needOnline = GetDynamicConfig()->SafeOnlineNodeCount;
    int gotOnline = nodeTracker->GetOnlineNodeCount();
    if (gotOnline < needOnline) {
        if (!Enabled_ || *Enabled_) {
            YT_LOG_INFO("Chunk replicator disabled: too few online nodes, needed >= %v but got %v",
                needOnline,
                gotOnline);
        }
        Enabled_ = false;
        return;
    }

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    const auto& statistics = multicellManager->GetClusterStatistics();
    int gotChunkCount = statistics.chunk_count();
    int gotLostChunkCount = statistics.lost_vital_chunk_count();
    int needLostChunkCount = GetDynamicConfig()->SafeLostChunkCount;
    if (gotChunkCount > 0) {
        double needFraction = GetDynamicConfig()->SafeLostChunkFraction;
        double gotFraction = (double) gotLostChunkCount / gotChunkCount;
        if (gotFraction > needFraction) {
            if (!Enabled_ || *Enabled_) {
                YT_LOG_INFO("Chunk replicator disabled: too many lost chunks, fraction needed <= %v but got %v",
                    needFraction,
                    gotFraction);
            }
            Enabled_ = false;
            return;
        }
    }

    if (gotLostChunkCount > needLostChunkCount) {
        if (!Enabled_ || *Enabled_) {
            YT_LOG_INFO("Chunk replicator disabled: too many lost chunks, needed <= %v but got %v",
                needLostChunkCount,
                gotLostChunkCount);
        }
        Enabled_ = false;
        return;
    }

    if (!Enabled_ || !*Enabled_) {
        YT_LOG_INFO("Chunk replicator enabled");
    }
    Enabled_ = true;
}

void TChunkReplicator::OnCheckEnabledSecondary()
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    auto primaryCellTag = multicellManager->GetPrimaryCellTag();
    auto channel = multicellManager->GetMasterChannelOrThrow(primaryCellTag, EPeerKind::Leader);

    TObjectServiceProxy proxy(channel);

    auto req = TYPathProxy::Get("//sys/@chunk_replicator_enabled");
    auto rsp = WaitFor(proxy.Execute(req))
        .ValueOrThrow();

    auto value = ConvertTo<bool>(TYsonString(rsp->value()));
    if (!Enabled_ || value != *Enabled_) {
        if (value) {
            YT_LOG_INFO("Chunk replicator enabled at primary master");
        } else {
            YT_LOG_INFO("Chunk replicator disabled at primary master");
        }
        Enabled_ = value;
    }
}

void TChunkReplicator::TryRescheduleChunkRemoval(const TJobPtr& unsucceededJob)
{
    if (unsucceededJob->GetType() == EJobType::RemoveChunk &&
        !unsucceededJob->Error().FindMatching(NChunkClient::EErrorCode::NoSuchChunk))
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeByAddress(unsucceededJob->NodeAddress());
        // If job was aborted due to node unregistration, do not reschedule job.
        if (!node->ReportedDataNodeHeartbeat()) {
            return;
        }
        const auto& replica = unsucceededJob->GetChunkIdWithIndexes();
        node->AddToChunkRemovalQueue(replica);
    }
}

void TChunkReplicator::OnProfiling(TSensorBuffer* buffer)
{
    buffer->AddGauge("/blob_refresh_queue_size", BlobRefreshScanner_->GetQueueSize());
    buffer->AddGauge("/blob_requisition_update_queue_size", BlobRequisitionUpdateScanner_->GetQueueSize());
    buffer->AddGauge("/journal_refresh_queue_size", JournalRefreshScanner_->GetQueueSize());
    buffer->AddGauge("/journal_requisition_update_queue_size", JournalRequisitionUpdateScanner_->GetQueueSize());

    buffer->AddGauge("/lost_chunk_count", LostChunks_.size());
    buffer->AddGauge("/lost_vital_chunk_count", LostVitalChunks_.size());
    buffer->AddGauge("/overreplicated_chunk_count", OverreplicatedChunks_.size());
    buffer->AddGauge("/underreplicated_chunk_count", UnderreplicatedChunks_.size());
    buffer->AddGauge("/data_missing_chunk_count", DataMissingChunks_.size());
    buffer->AddGauge("/parity_missing_chunk_count", ParityMissingChunks_.size());
    buffer->AddGauge("/precarious_chunk_count", PrecariousChunks_.size());
    buffer->AddGauge("/precarious_vital_chunk_count", PrecariousVitalChunks_.size());
    buffer->AddGauge("/quorum_missing_chunk_count", QuorumMissingChunks_.size());
    buffer->AddGauge("/unsafely_placed_chunk_count", UnsafelyPlacedChunks_.size());
    buffer->AddGauge("/inconsistently_placed_chunk_count", InconsistentlyPlacedChunks_.size());

    for (auto jobType : TEnumTraits<EJobType>::GetDomainValues()) {
        if (jobType >= NJobTrackerClient::FirstMasterJobType &&
            jobType <= NJobTrackerClient::LastMasterJobType)
        {
            TWithTagGuard tagGuard(buffer, "job_type", FormatEnum(jobType));
            buffer->AddCounter("/misscheduled_jobs", MisscheduledJobs_[jobType]);
        }
    }

    auto now = NProfiling::GetInstant();
    if (now - LastPerNodeProfilingTime_ >= GetDynamicConfig()->DestroyedReplicasProfilingPeriod) {
        i64 pullReplicationQueueSize = 0;
        i64 chunksBeingPulledCount = 0;
        i64 pushReplicationTargetNodeCount = 0;
        for (auto [_, node] : Bootstrap_->GetNodeTracker()->Nodes()) {
            for (const auto& queue : node->ChunkPullReplicationQueues()) {
                pullReplicationQueueSize += std::ssize(queue);
            }
            chunksBeingPulledCount += std::ssize(node->ChunksBeingPulled());
            pushReplicationTargetNodeCount += std::ssize(node->PushReplicationTargetNodeIds());

            TWithTagGuard tagGuard(buffer, "node_address", node->GetDefaultAddress());
            buffer->AddGauge("/destroyed_replicas_size", node->DestroyedReplicas().size());
            buffer->AddGauge("/removal_queue_size", node->ChunkRemovalQueue().size());
        }
        buffer->AddGauge("/pull_replication_queue_size", pullReplicationQueueSize);
        buffer->AddGauge("/crp_chunks_being_pulled_count", chunksBeingPulledCount);
        buffer->AddGauge("/crp_push_replication_target_node_count", pushReplicationTargetNodeCount);

        LastPerNodeProfilingTime_ = now;
    }
}

void TChunkReplicator::OnJobWaiting(const TJobPtr& job, IJobControllerCallbacks* callbacks)
{
    // In Replicator we don't distinguish between running and waiting jobs.
    OnJobRunning(job, callbacks);
}

void TChunkReplicator::OnJobRunning(const TJobPtr& job, IJobControllerCallbacks* callbacks)
{
    if (TInstant::Now() - job->GetStartTime() > GetDynamicConfig()->JobTimeout) {
        YT_LOG_WARNING("Job timed out, aborting (JobId: %v, JobType: %v, Address: %v, Duration: %v, ChunkId: %v)",
            job->GetJobId(),
            job->GetType(),
            job->NodeAddress(),
            TInstant::Now() - job->GetStartTime(),
            job->GetChunkIdWithIndexes());

        callbacks->AbortJob(job);
    }
}

void TChunkReplicator::RemoveChunkFromPullReplicationQueue(const TJobPtr& job)
{
    if (job->GetType() != EJobType::ReplicateChunk) {
        return;
    }

    const auto& chunkIdWithIndexes = job->GetChunkIdWithIndexes();
    auto chunkId = chunkIdWithIndexes.Id;
    auto replicationJob = StaticPointerCast<TReplicationJob>(job);
    for (const auto& replica : replicationJob->TargetReplicas()) {
        auto* node = replica.GetPtr();
        node->ChunksBeingPulled().erase(chunkId);
    }

    MaybeRemoveFromPullReplicationSet(replicationJob->GetTargetNodeId(), chunkId, chunkIdWithIndexes.MediumIndex);
}

void TChunkReplicator::OnJobCompleted(const TJobPtr& job)
{
    RemoveChunkFromPullReplicationQueue(job);
}

void TChunkReplicator::OnJobAborted(const TJobPtr& job)
{
    RemoveChunkFromPullReplicationQueue(job);
    TryRescheduleChunkRemoval(job);
}

void TChunkReplicator::OnJobFailed(const TJobPtr& job)
{
    RemoveChunkFromPullReplicationQueue(job);
    TryRescheduleChunkRemoval(job);
}

void TChunkReplicator::ScheduleRequisitionUpdate(TChunkList* chunkList)
{
    // Fast path.
    if (!RequisitionUpdateRunning_) {
        return;
    }

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
            YT_VERIFY(IsObjectAlive(Root_));
            auto callbacks = CreateAsyncChunkTraverserContext(
                Bootstrap_,
                NCellMaster::EAutomatonThreadQueue::ChunkRequisitionUpdateTraverser);
            TraverseChunkTree(std::move(callbacks), this, Root_);
        }

    private:
        TBootstrap* const Bootstrap_;
        const TChunkReplicatorPtr Owner_;
        TChunkList* const Root_;

        bool OnChunk(
            TChunk* chunk,
            TChunkList* /*parent*/,
            std::optional<i64> /*rowIndex*/,
            std::optional<int> /*tabletIndex*/,
            const NChunkClient::TReadLimit& /*startLimit*/,
            const NChunkClient::TReadLimit& /*endLimit*/,
            const TChunkViewModifier* /*modifier*/) override
        {
            Owner_->ScheduleRequisitionUpdate(chunk);
            return true;
        }

        bool OnChunkView(TChunkView* /*chunkView*/) override
        {
            return false;
        }

        bool OnDynamicStore(
            TDynamicStore* /*dynamicStore*/,
            std::optional<int> /*tabletIndex*/,
            const NChunkClient::TReadLimit& /*startLimit*/,
            const NChunkClient::TReadLimit& /*endLimit*/) override
        {
            return true;
        }

        void OnFinish(const TError& error) override
        {
            if (!error.IsOK()) {
                // Try restarting.
                Run();
            } else {
                Owner_->ConfirmChunkListRequisitionTraverseFinished(Root_);
            }
        }
    };

    New<TVisitor>(Bootstrap_, this, chunkList)->Run();
}

void TChunkReplicator::ScheduleRequisitionUpdate(TChunk* chunk)
{
    // Fast path.
    if (!RequisitionUpdateRunning_) {
        return;
    }

    if (!IsObjectAlive(chunk)) {
        return;
    }

    GetChunkRequisitionUpdateScanner(chunk)->EnqueueChunk(chunk);
}

void TChunkReplicator::OnRequisitionUpdate()
{
    if (!RequisitionUpdateRunning_) {
        return;
    }

    if (!Bootstrap_->GetHydraFacade()->GetHydraManager()->IsActiveLeader()) {
        return;
    }

    if (!GetDynamicConfig()->EnableChunkRequisitionUpdate) {
        YT_LOG_DEBUG("Chunk requisition update disabled");
        return;
    }

    TReqUpdateChunkRequisition request;
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    request.set_cell_tag(multicellManager->GetCellTag());

    YT_LOG_DEBUG("Chunk requisition update iteration started");

    TmpRequisitionRegistry_.Clear();

    auto doUpdateChunkRequisition = [&] (
        const std::unique_ptr<TChunkScanner>& scanner,
        int* const totalCount,
        int* const aliveCount,
        int maxChunksPerRequisitionUpdate,
        TDuration maxTimePerRequisitionUpdate)
    {
        NProfiling::TWallTimer timer;

        while (*totalCount < maxChunksPerRequisitionUpdate && scanner->HasUnscannedChunk()) {
            if (timer.GetElapsedTime() > maxTimePerRequisitionUpdate) {
                break;
            }

            ++(*totalCount);
            auto* chunk = scanner->DequeueChunk();
            if (!chunk) {
                continue;
            }

            ComputeChunkRequisitionUpdate(chunk, &request);
            ++(*aliveCount);
        }
    };

    int totalBlobCount = 0;
    int aliveBlobCount = 0;
    int totalJournalCount = 0;
    int aliveJournalCount = 0;

    YT_PROFILE_TIMING("/chunk_server/requisition_update_time") {
        ClearChunkRequisitionCache();
        doUpdateChunkRequisition(
            BlobRequisitionUpdateScanner_,
            &totalBlobCount,
            &aliveBlobCount,
            GetDynamicConfig()->MaxBlobChunksPerRequisitionUpdate,
            GetDynamicConfig()->MaxTimePerBlobChunkRequisitionUpdate);
        doUpdateChunkRequisition(
            JournalRequisitionUpdateScanner_,
            &totalJournalCount,
            &aliveJournalCount,
            GetDynamicConfig()->MaxJournalChunksPerRequisitionUpdate,
            GetDynamicConfig()->MaxTimePerJournalChunkRequisitionUpdate);
    }

    FillChunkRequisitionDict(&request, TmpRequisitionRegistry_);

    YT_LOG_DEBUG("Chunk requisition update iteration completed (TotalBlobCount: %v, AliveBlobCount: %v, TotalJournalCount: %v, AliveJournalCount: %v, UpdateCount: %v)",
        totalBlobCount,
        aliveBlobCount,
        totalJournalCount,
        aliveJournalCount,
        request.updates_size());

    if (request.updates_size() > 0) {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto asyncResult = chunkManager
            ->CreateUpdateChunkRequisitionMutation(request)
            ->CommitAndLog(Logger);
        Y_UNUSED(WaitFor(asyncResult));
    }
}

void TChunkReplicator::ComputeChunkRequisitionUpdate(TChunk* chunk, TReqUpdateChunkRequisition* request)
{
    auto oldGlobalRequisitionIndex = chunk->GetLocalRequisitionIndex();
    auto newRequisition = ComputeChunkRequisition(chunk);
    auto* globalRegistry = GetChunkRequisitionRegistry();
    auto newGlobalRequisitionIndex = globalRegistry->Find(newRequisition);
    if (!newGlobalRequisitionIndex || *newGlobalRequisitionIndex != oldGlobalRequisitionIndex) {
        auto* update = request->add_updates();
        ToProto(update->mutable_chunk_id(), chunk->GetId());
        // Don't mix up true (global) and temporary (ephemeral) requisition indexes.
        auto newTmpRequisitionIndex = TmpRequisitionRegistry_.GetOrCreateIndex(newRequisition);
        update->set_chunk_requisition_index(newTmpRequisitionIndex);
    }
}

TChunkRequisition TChunkReplicator::ComputeChunkRequisition(const TChunk* chunk)
{
    if (CanServeRequisitionFromCache(chunk)) {
        return GetRequisitionFromCache(chunk);
    }

    bool found = false;
    TChunkRequisition requisition;

    // Unique number used to distinguish already visited chunk lists.
    auto mark = TChunkList::GenerateVisitMark();

    // BFS queue. Try to avoid allocations.
    TCompactVector<TChunkList*, 64> queue;
    size_t frontIndex = 0;

    auto enqueue = [&] (TChunkList* chunkList) {
        if (chunkList->GetVisitMark() != mark) {
            chunkList->SetVisitMark(mark);
            queue.push_back(chunkList);
        }
    };

    auto enqueueAdjustedParent = [&] (TChunkList* parent) {
        auto* adjustedParent = FollowParentLinks(parent);
        if (adjustedParent) {
            enqueue(adjustedParent);
        }
    };

    // Put seeds into the queue.
    for (auto [parent, cardinality] : chunk->Parents()) {
        switch (parent->GetType()) {
            case EObjectType::ChunkList:
                enqueueAdjustedParent(parent->AsChunkList());
                break;

            case EObjectType::ChunkView:
                for (auto* chunkViewParent : parent->AsChunkView()->Parents()) {
                    enqueueAdjustedParent(chunkViewParent);
                }
                break;

            default:
                YT_ABORT();
        }
    }

    // The main BFS loop.
    while (frontIndex < queue.size()) {
        auto* chunkList = queue[frontIndex++];

        // Examine owners, if any.
        for (const auto* owningNode : chunkList->TrunkOwningNodes()) {
            auto* account = owningNode->GetAccount();
            if (account) {
                requisition.AggregateWith(owningNode->Replication(), account, true);
            }

            found = true;
        }
        // Proceed to parents.
        for (auto* parent : chunkList->Parents()) {
            enqueueAdjustedParent(parent);
        }
    }

    if (chunk->IsErasure()) {
        static_assert(MinReplicationFactor <= 1 && 1 <= MaxReplicationFactor,
                     "Replication factor limits are incorrect.");
        requisition.ForceReplicationFactor(1);
    }

    if (found) {
        YT_ASSERT(requisition.ToReplication().IsValid());
    } else {
        // Chunks that aren't linked to any trunk owner are assigned empty requisition.
        // This doesn't mean the replicator will act upon it, though, as the chunk will
        // remember its last non-empty aggregated requisition.
        requisition = GetChunkRequisitionRegistry()->GetRequisition(EmptyChunkRequisitionIndex);
    }

    CacheRequisition(chunk, requisition);

    return requisition;
}

void TChunkReplicator::ClearChunkRequisitionCache()
{
    ChunkRequisitionCache_.LastChunkParents.clear();
    ChunkRequisitionCache_.LastChunkUpdatedRequisition = std::nullopt;
    ChunkRequisitionCache_.LastErasureChunkUpdatedRequisition = std::nullopt;
}

bool TChunkReplicator::CanServeRequisitionFromCache(const TChunk* chunk)
{
    if (chunk->IsStaged() || chunk->Parents() != ChunkRequisitionCache_.LastChunkParents) {
        return false;
    }

    return chunk->IsErasure()
        ? ChunkRequisitionCache_.LastErasureChunkUpdatedRequisition.operator bool()
        : ChunkRequisitionCache_.LastChunkUpdatedRequisition.operator bool();
}

TChunkRequisition TChunkReplicator::GetRequisitionFromCache(const TChunk* chunk)
{
    return chunk->IsErasure()
        ? *ChunkRequisitionCache_.LastErasureChunkUpdatedRequisition
        : *ChunkRequisitionCache_.LastChunkUpdatedRequisition;
}

void TChunkReplicator::CacheRequisition(const TChunk* chunk, const TChunkRequisition& requisition)
{
    if (chunk->IsStaged()) {
        return;
    }

    if (ChunkRequisitionCache_.LastChunkParents != chunk->Parents()) {
        ClearChunkRequisitionCache();
        ChunkRequisitionCache_.LastChunkParents = chunk->Parents();
    }

    if (chunk->IsErasure()) {
        ChunkRequisitionCache_.LastErasureChunkUpdatedRequisition = requisition;
    } else {
        ChunkRequisitionCache_.LastChunkUpdatedRequisition = requisition;
    }
}

void TChunkReplicator::ConfirmChunkListRequisitionTraverseFinished(TChunkList* chunkList)
{
    auto chunkListId = chunkList->GetId();
    YT_LOG_DEBUG("Chunk list requisition traverse finished (ChunkListId: %v)",
        chunkListId);
    ChunkListIdsWithFinishedRequisitionTraverse_.push_back(chunkListId);
}

void TChunkReplicator::OnFinishedRequisitionTraverseFlush()
{
    if (!RequisitionUpdateRunning_) {
        return;
    }

    if (!Bootstrap_->GetHydraFacade()->GetHydraManager()->IsActiveLeader()) {
        return;
    }

    if (ChunkListIdsWithFinishedRequisitionTraverse_.empty()) {
        return;
    }

    YT_LOG_DEBUG("Flushing finished chunk lists requisition traverse confirmations (Count: %v)",
        ChunkListIdsWithFinishedRequisitionTraverse_.size());

    TReqConfirmChunkListsRequisitionTraverseFinished request;
    ToProto(request.mutable_chunk_list_ids(), ChunkListIdsWithFinishedRequisitionTraverse_);
    ChunkListIdsWithFinishedRequisitionTraverse_.clear();

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto asyncResult = chunkManager
        ->CreateConfirmChunkListsRequisitionTraverseFinishedMutation(request)
        ->CommitAndLog(Logger);
    Y_UNUSED(WaitFor(asyncResult));
}

TChunkList* TChunkReplicator::FollowParentLinks(TChunkList* chunkList)
{
    while (chunkList->TrunkOwningNodes().Empty()) {
        const auto& parents = chunkList->Parents();
        auto parentCount = parents.Size();
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

void TChunkReplicator::AddToChunkRepairQueue(TChunkPtrWithIndexes chunkWithIndexes, EChunkRepairQueue queue)
{
    YT_ASSERT(chunkWithIndexes.GetReplicaIndex() == GenericChunkReplicaIndex);
    YT_ASSERT(chunkWithIndexes.GetState() == EChunkReplicaState::Generic);
    auto* chunk = chunkWithIndexes.GetPtr();
    int mediumIndex = chunkWithIndexes.GetMediumIndex();
    YT_VERIFY(chunk->GetRepairQueueIterator(mediumIndex, queue) == TChunkRepairQueueIterator());
    auto& chunkRepairQueue = ChunkRepairQueue(mediumIndex, queue);
    auto it = chunkRepairQueue.insert(chunkRepairQueue.end(), chunkWithIndexes);
    chunk->SetRepairQueueIterator(mediumIndex, queue, it);
}

void TChunkReplicator::RemoveFromChunkRepairQueues(TChunkPtrWithIndexes chunkWithIndexes)
{
    YT_ASSERT(chunkWithIndexes.GetReplicaIndex() == GenericChunkReplicaIndex);
    YT_ASSERT(chunkWithIndexes.GetState() == EChunkReplicaState::Generic);
    auto* chunk = chunkWithIndexes.GetPtr();
    int mediumIndex = chunkWithIndexes.GetMediumIndex();
    for (auto queue : TEnumTraits<EChunkRepairQueue>::GetDomainValues()) {
        auto it = chunk->GetRepairQueueIterator(mediumIndex, queue);
        if (it != TChunkRepairQueueIterator()) {
            ChunkRepairQueue(mediumIndex, queue).erase(it);
            chunk->SetRepairQueueIterator(mediumIndex, queue, TChunkRepairQueueIterator());
        }
    }
}

void TChunkReplicator::FlushEndorsementQueue()
{
    if (ChunkIdsPendingEndorsementRegistration_.empty()) {
        return;
    }

    NProto::TReqRegisterChunkEndorsements req;
    ToProto(req.mutable_chunk_ids(), ChunkIdsPendingEndorsementRegistration_);
    ChunkIdsPendingEndorsementRegistration_.clear();

    YT_LOG_DEBUG("Scheduled chunk endorsement registration (EndorsementCount: %v)",
        req.chunk_ids_size());

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    // Fire-and-forget. Mutation commit failure indicates the epoch change,
    // and that results in refreshing all chunks once again.
    chunkManager
        ->CreateRegisterChunkEndorsementsMutation(req)
        ->CommitAndLog(Logger);
}

const std::unique_ptr<TChunkScanner>& TChunkReplicator::GetChunkRefreshScanner(TChunk* chunk) const
{
    return chunk->IsJournal() ? JournalRefreshScanner_ : BlobRefreshScanner_;
}

const std::unique_ptr<TChunkScanner>& TChunkReplicator::GetChunkRequisitionUpdateScanner(TChunk* chunk) const
{
    return chunk->IsJournal() ? JournalRequisitionUpdateScanner_ : BlobRequisitionUpdateScanner_;
}

TChunkRequisitionRegistry* TChunkReplicator::GetChunkRequisitionRegistry()
{
    return Bootstrap_->GetChunkManager()->GetChunkRequisitionRegistry();
}

TChunkRepairQueue& TChunkReplicator::ChunkRepairQueue(int mediumIndex, EChunkRepairQueue queue)
{
    return ChunkRepairQueues(queue)[mediumIndex];
}

std::array<TChunkRepairQueue, MaxMediumCount>& TChunkReplicator::ChunkRepairQueues(EChunkRepairQueue queue)
{
    switch (queue) {
        case EChunkRepairQueue::Missing:
            return MissingPartChunkRepairQueues_;
        case EChunkRepairQueue::Decommissioned:
            return DecommissionedPartChunkRepairQueues_;
        default:
            YT_ABORT();
    }
}

TDecayingMaxMinBalancer<int, double>& TChunkReplicator::ChunkRepairQueueBalancer(EChunkRepairQueue queue)
{
    switch (queue) {
        case EChunkRepairQueue::Missing:
            return MissingPartChunkRepairQueueBalancer_;
        case EChunkRepairQueue::Decommissioned:
            return DecommissionedPartChunkRepairQueueBalancer_;
        default:
            YT_ABORT();
    }
}

const TDynamicChunkManagerConfigPtr& TChunkReplicator::GetDynamicConfig() const
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    return configManager->GetConfig()->ChunkManager;
}

void TChunkReplicator::OnDynamicConfigChanged(TDynamicClusterConfigPtr oldConfig)
{
    if (RefreshExecutor_) {
        RefreshExecutor_->SetPeriod(GetDynamicConfig()->ChunkRefreshPeriod);
    }
    if (RequisitionUpdateExecutor_) {
        RequisitionUpdateExecutor_->SetPeriod(GetDynamicConfig()->ChunkRequisitionUpdatePeriod);
    }
    if (FinishedRequisitionTraverseFlushExecutor_) {
        FinishedRequisitionTraverseFlushExecutor_->SetPeriod(GetDynamicConfig()->FinishedChunkListsRequisitionTraverseFlushPeriod);
    }

    if (!GetDynamicConfig()->ConsistentReplicaPlacement->Enable &&
        oldConfig->ChunkManager->ConsistentReplicaPlacement->Enable)
    {
        InconsistentlyPlacedChunks_.clear();
    }
}

bool TChunkReplicator::IsConsistentChunkPlacementEnabled() const
{
    return GetDynamicConfig()->ConsistentReplicaPlacement->Enable;
}


bool TChunkReplicator::UsePullReplication(TChunk* chunk) const
{
    return chunk->HasConsistentReplicaPlacementHash() &&
        IsConsistentChunkPlacementEnabled() &&
        GetDynamicConfig()->ConsistentReplicaPlacement->EnablePullReplication;
}

void TChunkReplicator::StartRefresh(int shardIndex)
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    BlobRefreshScanner_->Start(shardIndex, chunkManager->GetGlobalBlobChunkScanDescriptor(shardIndex));
    JournalRefreshScanner_->Start(shardIndex, chunkManager->GetGlobalJournalChunkScanDescriptor(shardIndex));

    YT_VERIFY(!RefreshRunning_.test(shardIndex));
    RefreshRunning_.set(shardIndex);

    auto& jobEpoch = JobEpochs_[shardIndex];
    YT_VERIFY(jobEpoch == InvalidJobEpoch);
    jobEpoch = JobRegistry_->StartEpoch();
    SetRefreshEpoch(shardIndex, jobEpoch);

    YT_LOG_INFO("Chunk refresh started (ShardIndex: %v, JobEpoch: %v)",
        shardIndex,
        jobEpoch);
}

void TChunkReplicator::StopRefresh(int shardIndex)
{
    if (!RefreshRunning_.test(shardIndex)) {
        return;
    }
    RefreshRunning_.reset(shardIndex);

    BlobRefreshScanner_->Stop(shardIndex);
    JournalRefreshScanner_->Stop(shardIndex);

    auto clearChunkSetShard = [&] (TShardedChunkSet& set) {
        THashSet<TChunk*> shard;
        std::swap(set.MutableShard(shardIndex), shard);

        // Shard may be heavy, so we offload its destruction into
        // separate thread.
        NRpc::TDispatcher::Get()->GetHeavyInvoker()->Invoke(
            BIND([shard = std::move(shard)] { }));
    };
    clearChunkSetShard(LostChunks_);
    clearChunkSetShard(LostVitalChunks_);
    clearChunkSetShard(DataMissingChunks_);
    clearChunkSetShard(ParityMissingChunks_);
    clearChunkSetShard(PrecariousChunks_);
    clearChunkSetShard(PrecariousVitalChunks_);
    clearChunkSetShard(UnderreplicatedChunks_);
    clearChunkSetShard(PrecariousVitalChunks_);
    clearChunkSetShard(UnderreplicatedChunks_);
    clearChunkSetShard(OverreplicatedChunks_);
    clearChunkSetShard(QuorumMissingChunks_);
    clearChunkSetShard(UnsafelyPlacedChunks_);
    clearChunkSetShard(InconsistentlyPlacedChunks_);

    // |OldestPartMissingChunks| should be ordered, so we use std::set
    // instead of sharded set for it. However this set is always small,
    // so O(n log n) time for shard removal is OK.
    TOldestPartMissingChunkSet newOldestPartMissingChunks;
    for (auto* chunk : OldestPartMissingChunks_) {
        if (chunk->GetShardIndex() != shardIndex) {
            newOldestPartMissingChunks.insert(chunk);
        }
    }
    OldestPartMissingChunks_ = std::move(newOldestPartMissingChunks);

    auto& jobEpoch = JobEpochs_[shardIndex];
    YT_VERIFY(jobEpoch != InvalidJobEpoch);
    JobRegistry_->OnEpochFinished(jobEpoch);
    jobEpoch = InvalidJobEpoch;

    YT_LOG_INFO("Chunk refresh stopped (ShardIndex: %v, JobEpoch: %v)",
        shardIndex,
        jobEpoch);
}

void TChunkReplicator::StartRequisitionUpdate()
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
        BlobRequisitionUpdateScanner_->Start(shardIndex, chunkManager->GetGlobalBlobChunkScanDescriptor(shardIndex));
        JournalRequisitionUpdateScanner_->Start(shardIndex, chunkManager->GetGlobalJournalChunkScanDescriptor(shardIndex));
    }

    YT_VERIFY(!std::exchange(RequisitionUpdateRunning_, true));

    YT_LOG_INFO("Chunk requisition update started");
}

void TChunkReplicator::StopRequisitionUpdate()
{
    if (std::exchange(RequisitionUpdateRunning_, false)) {
        return;
    }

    for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
        BlobRequisitionUpdateScanner_->Stop(shardIndex);
        JournalRequisitionUpdateScanner_->Stop(shardIndex);
    }

    YT_LOG_INFO("Chunk requisition update stopped");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
