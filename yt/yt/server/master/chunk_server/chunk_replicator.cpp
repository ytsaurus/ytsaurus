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
#include "domestic_medium.h"
#include "helpers.h"
#include "data_node_tracker.h"
#include "chunk_manager.h"
#include "refresh_epoch.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/world_initializer.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/proto/multicell_manager.pb.h>

#include <yt/yt/server/master/cypress_server/node.h>
#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/incumbent_server/incumbent_manager.h>

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
using namespace NIncumbentClient;
using namespace NObjectServer;
using namespace NChunkServer::NProto;
using namespace NCellMaster;
using namespace NTransactionClient;

using NChunkClient::TLegacyReadLimit;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TReplicationJob
    : public TJob
{
public:
    DEFINE_BYREF_RO_PROPERTY(TNodePtrWithReplicaAndMediumIndexList, TargetReplicas);
    DEFINE_BYVAL_RO_PROPERTY(TNodeId, TargetNodeId);

public:
    TReplicationJob(
        TJobId jobId,
        TJobEpoch jobEpoch,
        TNode* node,
        TChunkPtrWithReplicaIndex chunkWithIndex,
        const TNodePtrWithReplicaAndMediumIndexList& targetReplicas,
        TNodeId targetNodeId)
        : TJob(
            jobId,
            EJobType::ReplicateChunk,
            jobEpoch,
            node,
            TReplicationJob::GetResourceUsage(chunkWithIndex.GetPtr()),
            TChunkIdWithIndexes(
                chunkWithIndex.GetPtr()->GetId(),
                chunkWithIndex.GetReplicaIndex(),
                AllMediaIndex))
        , TargetReplicas_(targetReplicas)
        , TargetNodeId_(targetNodeId)
    { }

    bool FillJobSpec(TBootstrap* /*bootstrap*/, TJobSpec* jobSpec) const override
    {
        auto* jobSpecExt = jobSpec->MutableExtension(TReplicateChunkJobSpecExt::replicate_chunk_job_spec_ext);
        ToProto(jobSpecExt->mutable_chunk_id(), EncodeChunkId(ChunkIdWithIndexes_));
        jobSpecExt->set_source_medium_index(ChunkIdWithIndexes_.MediumIndex);
        jobSpecExt->set_is_pull_replication_job(TargetNodeId_ != InvalidNodeId);

        NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());
        for (auto replica : TargetReplicas_) {
            jobSpecExt->add_target_replicas(ToProto<ui64>(replica));
            builder.Add(replica);
        }

        return true;
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
    // NB: This field is used for job rescheduling.
    DEFINE_BYREF_RO_PROPERTY(TChunkLocationUuid, ChunkLocationUuid);

public:
    TRemovalJob(
        TJobId jobId,
        TJobEpoch jobEpoch,
        TNode* node,
        TChunk* chunk,
        TChunkIdWithIndexes replica,
        TChunkLocationUuid locationUuid)
        : TJob(
            jobId,
            EJobType::RemoveChunk,
            jobEpoch,
            node,
            TRemovalJob::GetResourceUsage(),
            replica)
        , ChunkLocationUuid_(locationUuid)
        , Chunk_(chunk)
    { }

    bool FillJobSpec(TBootstrap* bootstrap, TJobSpec* jobSpec) const override
    {
        auto* jobSpecExt = jobSpec->MutableExtension(TRemoveChunkJobSpecExt::remove_chunk_job_spec_ext);
        ToProto(jobSpecExt->mutable_chunk_id(), EncodeChunkId(ChunkIdWithIndexes_));
        jobSpecExt->set_medium_index(ChunkIdWithIndexes_.MediumIndex);
        if (!IsObjectAlive(Chunk_)) {
            jobSpecExt->set_chunk_is_dead(true);
            return true;
        }

        bool isErasure = Chunk_->IsErasure();
        for (auto replica : Chunk_->StoredReplicas()) {
            if (GetChunkLocationNode(replica)->GetDefaultAddress() == NodeAddress_) {
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

        jobSpecExt->set_replicas_expiration_deadline(ToProto<ui64>(chunkRemovalJobExpirationDeadline));

        return true;
    }

private:
    const TEphemeralObjectPtr<TChunk> Chunk_;

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
    DEFINE_BYREF_RO_PROPERTY(TNodePtrWithReplicaAndMediumIndexList, TargetReplicas);

public:
    TRepairJob(
        TJobId jobId,
        TJobEpoch jobEpoch,
        TNode* node,
        i64 jobMemoryUsage,
        TChunk* chunk,
        const TNodePtrWithReplicaAndMediumIndexList& targetReplicas,
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

    bool FillJobSpec(TBootstrap* /*bootstrap*/, TJobSpec* jobSpec) const override
    {
        if (!IsObjectAlive(Chunk_)) {
            return false;
        }

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

        return true;
    }

private:
    const TEphemeralObjectPtr<TChunk> Chunk_;
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
    : TShardedIncumbentBase(
        bootstrap->GetIncumbentManager(),
        EIncumbentType::ChunkReplicator)
    , Config_(std::move(config))
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

    const auto& incumbentManager = Bootstrap_->GetIncumbentManager();
    incumbentManager->RegisterIncumbent(this);

    std::fill(JobEpochs_.begin(), JobEpochs_.end(), InvalidJobEpoch);
    for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
        SetRefreshEpoch(shardIndex, InvalidRefreshEpoch);
    }

    YT_VERIFY(GetShardCount() == ChunkShardCount);
}

TChunkReplicator::~TChunkReplicator() = default;

void TChunkReplicator::OnEpochStarted()
{
    RefreshExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkRefresher),
        BIND(&TChunkReplicator::OnRefresh, MakeWeak(this)),
        GetDynamicConfig()->ChunkRefreshPeriod);
    RefreshExecutor_->Start();

    EnabledCheckExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Periodic),
        BIND(&TChunkReplicator::OnCheckEnabled, MakeWeak(this)),
        GetDynamicConfig()->ReplicatorEnabledCheckPeriod);
    EnabledCheckExecutor_->Start();

    // Just in case.
    Enabled_ = false;
}

void TChunkReplicator::OnEpochFinished()
{
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

    for (auto queueKind : TEnumTraits<EChunkRepairQueue>::GetDomainValues()) {
        for (auto& queue : ChunkRepairQueues(queueKind)) {
            for (auto chunkWithIndexes : queue) {
                chunkWithIndexes.GetPtr()->SetRepairQueueIterator(
                    chunkWithIndexes.GetMediumIndex(),
                    queueKind,
                    TChunkRepairQueueIterator());
            }
            queue.clear();
        }
        ChunkRepairQueueBalancer(queueKind).ResetWeights();
    }

    ChunkIdsPendingEndorsementRegistration_.clear();

    for (auto jobType : TEnumTraits<EJobType>::GetDomainValues()) {
        MisscheduledJobs_[jobType] = 0;
    }

    Enabled_ = false;
}

void TChunkReplicator::OnLeadingStarted()
{
    StartRequisitionUpdate();

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
}

void TChunkReplicator::OnLeadingFinished()
{
    StopRequisitionUpdate();

    ClearChunkRequisitionCache();
    TmpRequisitionRegistry_.Clear();
}

void TChunkReplicator::OnIncumbencyStarted(int shardIndex)
{
    TShardedIncumbentBase::OnIncumbencyStarted(shardIndex);

    StartRefresh(shardIndex);
}

void TChunkReplicator::OnIncumbencyFinished(int shardIndex)
{
    TShardedIncumbentBase::OnIncumbencyFinished(shardIndex);

    StopRefresh(shardIndex);
}

void TChunkReplicator::TouchChunk(TChunk* chunk)
{
    // Fast path.
    if (!ShouldProcessChunk(chunk)) {
        return;
    }

    TouchChunkInRepairQueues(chunk);
}

TCompactMediumMap<EChunkStatus> TChunkReplicator::ComputeChunkStatuses(TChunk* chunk)
{
    TCompactMediumMap<EChunkStatus> result;

    auto statistics = ComputeChunkStatistics(chunk);

    for (const auto& [mediumIndex, mediumStatistics] : statistics.PerMediumStatistics) {
        result[mediumIndex] = mediumStatistics.Status;
    }

    return result;
}

ECrossMediumChunkStatus TChunkReplicator::ComputeCrossMediumChunkStatus(TChunk* chunk)
{
    return ComputeChunkStatistics(chunk).Status;
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

    TCompactMediumMap<std::array<TChunkLocationList, ChunkReplicaIndexBound>> decommissionedReplicas;
    TCompactMediumMap<std::array<ui8, RackIndexBound>> perRackReplicaCounters;
    // TODO(gritukan): YT-16557.
    TCompactMediumMap<THashMap<const TDataCenter*, ui8>> perDataCenterReplicaCounters;

    // An arbitrary replica collocated with too may others within a single rack - per medium.
    TCompactMediumMap<TChunkLocationPtrWithReplicaInfo> unsafelyPlacedSealedReplicas;
    // An arbitrary replica that violates consistent placement requirements - per medium.
    TCompactMediumMap<std::array<TChunkLocation*, ChunkReplicaIndexBound>> inconsistentlyPlacedSealedReplicas;

    TCompactMediumMap<int> totalReplicaCounts;
    TCompactMediumMap<int> totalDecommissionedReplicaCounts;

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
        auto* chunkLocation = replica.GetPtr();
        auto* node = chunkLocation->GetNode();
        int replicaIndex = replica.GetReplicaIndex();
        int mediumIndex = replica.GetPtr()->GetEffectiveMediumIndex();
        auto& mediumStatistics = result.PerMediumStatistics[mediumIndex];

        replicaIndexes.set(replicaIndex);

        auto isReplicaSealed = !chunk->IsJournal() || replica.GetReplicaState() == EChunkReplicaState::Sealed;

        if (!isReplicaSealed) {
            totallySealed = false;
        }

        if (IsReplicaDecommissioned(replica.GetPtr()) || node->GetVisitMark(mediumIndex) == mark) {
            ++mediumStatistics.DecommissionedReplicaCount[replicaIndex];
            decommissionedReplicas[mediumIndex][replicaIndex].push_back(replica.GetPtr());
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
    TCompactMediumMap<NErasure::TPartIndexSet> mediumToErasedIndexes;
    TMediumSet activeMedia;

    const auto& chunkManager = Bootstrap_->GetChunkManager();

    for (const auto& entry : chunkReplication) {
        auto mediumIndex = entry.GetMediumIndex();
        auto* mediumBase = chunkManager->FindMediumByIndex(mediumIndex);
        YT_VERIFY(IsObjectAlive(mediumBase));

        // TODO(gritukan): Check replica presence here when
        // chunk will store offshore replicas.
        // For now, we just ignore such media.
        if (mediumBase->IsOffshore()) {
            continue;
        }

        auto* medium = mediumBase->AsDomestic();

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

TCompactMediumMap<TNodeList> TChunkReplicator::GetChunkConsistentPlacementNodes(const TChunk* chunk)
{
    if (!chunk->HasConsistentReplicaPlacementHash()) {
        return {};
    }

    if (!IsConsistentChunkPlacementEnabled()) {
        return {};
    }

    const auto& chunkManager = Bootstrap_->GetChunkManager();

    TCompactMediumMap<TNodeList> result;
    const auto chunkReplication = GetChunkAggregatedReplication(chunk);
    for (const auto& entry : chunkReplication) {
        auto mediumPolicy = entry.Policy();
        if (!mediumPolicy) {
            continue;
        }

        auto mediumIndex = entry.GetMediumIndex();
        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        YT_VERIFY(IsObjectAlive(medium));

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
    const std::array<TChunkLocationList, ChunkReplicaIndexBound>& decommissionedReplicas,
    TChunkLocationPtrWithReplicaInfo unsafelyPlacedSealedReplica,
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
                auto& removalReplicas = result.DecommissionedRemovalReplicas;
                removalReplicas.reserve(removalReplicas.size() + replicas.size());
                std::transform(
                    replicas.begin(),
                    replicas.end(),
                    std::back_inserter(result.DecommissionedRemovalReplicas),
                    [&] (auto* location) {
                        return TChunkLocationPtrWithReplicaIndex(location, index);
                    });
                result.Status |= EChunkStatus::Overreplicated;
            }

            if (replicaCount > targetReplicationFactor && decommissionedReplicaCount == 0) {
                result.Status |= EChunkStatus::Overreplicated | EChunkStatus::UnexpectedOverreplicated;
                result.BalancingRemovalIndexes.push_back(index);
            }

            if (replicaCount == 0 && decommissionedReplicaCount > 0 && !removalAdvised && std::ssize(decommissionedReplicas) > index) {
                const auto& replicas = decommissionedReplicas[index];
                // A replica may be "decommissioned" either because its node is
                // decommissioned or that node holds another part of the chunk (and that's
                // not allowed by the configuration). Let's distinguish these cases.
                auto isReplicaDecommissioned = [&] (TChunkLocation* replica) {
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

    if (unsafelyPlacedSealedReplica.GetPtr() &&
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
    const TCompactMediumMap<NErasure::TPartIndexSet>& mediumToErasedIndexes,
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
            if (medium->IsDomestic()) {
                transientMedia.set(medium->GetIndex(), medium->AsDomestic()->GetTransient());
            }
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

    TMediumMap<TChunkLocationPtrWithReplicaInfo> unsafelyPlacedReplicas;
    TMediumMap<std::array<ui8, RackIndexBound>> perRackReplicaCounters;
    // TODO(gritukan): YT-16557.
    TMediumMap<THashMap<const TDataCenter*, ui8>> perDataCenterReplicaCounters;

    // An arbitrary replica that violates consistent placement requirements - per medium.
    TMediumMap<TChunkLocationPtrWithReplicaIndex> inconsistentlyPlacedReplica;

    TMediumMap<int> replicaCount;
    TMediumMap<int> decommissionedReplicaCount;
    TMediumMap<TChunkLocationPtrWithReplicaIndexList> decommissionedReplicas;
    int totalReplicaCount = 0;
    int totalDecommissionedReplicaCount = 0;

    TMediumMap<TNodePtrWithReplicaAndMediumIndexList> missingReplicas;

    TMediumSet hasSealedReplica;
    bool hasSealedReplicas = false;
    bool totallySealed = chunk->IsSealed();

    auto consistentPlacementNodes = GetChunkConsistentPlacementNodes(chunk);
    auto storedReplicas = chunk->StoredReplicas();

    for (const auto& [mediumIndex, consistentNodes] : consistentPlacementNodes) {
        for (auto node : consistentNodes) {
            TNodePtrWithReplicaAndMediumIndex nodePtrWithIndexes(node, GenericChunkReplicaIndex, mediumIndex);
            auto it = std::find_if(
                storedReplicas.begin(),
                storedReplicas.end(),
                [&, mediumIndex = mediumIndex] (const auto& replica) {
                    return GetChunkLocationNode(replica) == node && replica.GetPtr()->GetEffectiveMediumIndex() == mediumIndex;
                });
            if (it == storedReplicas.end()) {
                missingReplicas[mediumIndex].push_back(nodePtrWithIndexes);
            }
        }
    }

    for (auto replica : chunk->StoredReplicas()) {
        auto* chunkLocation = replica.GetPtr();
        auto* node = chunkLocation->GetNode();
        auto mediumIndex = replica.GetPtr()->GetEffectiveMediumIndex();

        if (chunk->IsJournal() && replica.GetReplicaState() != EChunkReplicaState::Sealed) {
            totallySealed = false;
        } else {
            hasSealedReplica[mediumIndex] = true;
            hasSealedReplicas = true;
        }

        if (IsReplicaDecommissioned(replica.GetPtr())) {
            ++decommissionedReplicaCount[mediumIndex];
            decommissionedReplicas[mediumIndex].emplace_back(replica.GetPtr(), replica.GetReplicaIndex());
            ++totalDecommissionedReplicaCount;
        } else {
            ++replicaCount[mediumIndex];
            ++totalReplicaCount;
        }

        if (const auto* rack = replica.GetPtr()->GetNode()->GetRack()) {
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
                inconsistentlyPlacedReplica[mediumIndex] = TChunkLocationPtrWithReplicaIndex(
                    chunkLocation,
                    replica.GetReplicaIndex());
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

        // TODO(gritukan): Check replica presence here when
        // chunk will store offshore replicas.
        // For now, we just ignore such media.
        if (medium->IsOffshore()) {
            continue;
        }

        auto& mediumStatistics = results.PerMediumStatistics[mediumIndex];
        auto mediumTransient = medium->AsDomestic()->GetTransient();

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
    const TChunkLocationPtrWithReplicaIndexList& decommissionedReplicas,
    bool hasSealedReplica,
    bool totallySealed,
    TChunkLocationPtrWithReplicaInfo unsafelyPlacedReplica,
    TChunkLocationPtrWithReplicaIndex inconsistentlyPlacedReplica,
    const TNodePtrWithReplicaAndMediumIndexList& missingReplicas)
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
            if (replicaCount + decommissionedReplicaCount > replicationFactor) {
                result.Status |= EChunkStatus::Overreplicated;
                if (replicaCount > replicationFactor) {
                    result.Status |= EChunkStatus::UnexpectedOverreplicated;
                }
                if (inconsistentlyPlacedReplica.GetPtr()) {
                    result.DecommissionedRemovalReplicas.push_back(inconsistentlyPlacedReplica);
                } else if (decommissionedReplicaCount > 0) {
                    result.DecommissionedRemovalReplicas.insert(
                        result.DecommissionedRemovalReplicas.end(),
                        decommissionedReplicas.begin(),
                        decommissionedReplicas.end());
                } else {
                    result.BalancingRemovalIndexes.push_back(GenericChunkReplicaIndex);
                }
            }
        }
    }

    if (replicationFactor > 1 && unsafelyPlacedReplica.GetPtr() && None(result.Status & EChunkStatus::Overreplicated)) {
        result.Status |= EChunkStatus::UnsafelyPlaced;
        result.UnsafelyPlacedReplica = unsafelyPlacedReplica;
    }

    if (inconsistentlyPlacedReplica.GetPtr() && None(result.Status & EChunkStatus::Overreplicated)) {
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
    for (auto* location : node->ChunkLocations()) {
        YT_VERIFY(location->ChunkSealQueue().empty());
        YT_VERIFY(location->ChunkRemovalQueue().empty());
    }
    for (const auto& queue : node->ChunkPushReplicationQueues()) {
        YT_VERIFY(queue.empty());
    }

    // ChunksBeingPulled may still contain chunks and it is ok, as it is a responsibility
    // of source nodes to remove corresponding chunks.

    YT_VERIFY(node->PushReplicationTargetNodeIds().empty());
}

void TChunkReplicator::OnNodeUnregistered(TNode* node)
{
    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    node->ClearPushReplicationTargetNodeIds(nodeTracker);

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
    TChunkPtrWithReplicaIndex chunkWithIndex)
{
    auto chunkId = chunkWithIndex.GetPtr()->GetId();
    const auto& pullReplicationNodeIds = node->PushReplicationTargetNodeIds();
    if (auto it = pullReplicationNodeIds.find(chunkId); it != pullReplicationNodeIds.end()) {
        for (auto [mediumIndex, nodeId] : it->second) {
            UnrefChunkBeingPulled(nodeId, chunkId, mediumIndex);
        }
    }
    node->RemoveFromChunkReplicationQueues(chunkWithIndex);
}

void TChunkReplicator::OnReplicaRemoved(
    TChunkLocation* location,
    TChunkPtrWithReplicaIndex replica,
    ERemoveReplicaReason reason)
{
    auto* chunk = replica.GetPtr();
    auto* node = location->GetNode();

    // NB: It's OK to remove all replicas from replication queues here because
    // if some replica is removed we need to schedule chunk refresh anyway.
    RemoveFromChunkReplicationQueues(node, replica);
    if (reason != ERemoveReplicaReason::ChunkDestroyed) {
        auto chunkIdWithIndex = TChunkIdWithIndex(chunk->GetId(), replica.GetReplicaIndex());
        location->RemoveFromChunkRemovalQueue(chunkIdWithIndex);
    }
    if (chunk->IsJournal()) {
        location->RemoveFromChunkSealQueue(replica);
    }
}

void TChunkReplicator::UnrefChunkBeingPulled(
    TNodeId nodeId,
    TChunkId chunkId,
    int mediumIndex)
{
    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    if (auto* node = nodeTracker->FindNode(nodeId)) {
        node->UnrefChunkBeingPulled(chunkId, mediumIndex);
    }
}

bool TChunkReplicator::TryScheduleReplicationJob(
    IJobSchedulingContext* context,
    TChunkPtrWithReplicaIndex chunkWithIndex,
    TDomesticMedium* targetMedium,
    TNodeId targetNodeId)
{
    auto* sourceNode = context->GetNode();
    auto* chunk = chunkWithIndex.GetPtr();
    auto replicaIndex = chunkWithIndex.GetReplicaIndex();
    auto chunkId = chunk->GetId();
    auto mediumIndex = targetMedium->GetIndex();
    TJobPtr job;

    auto isPullReplicationJob = targetNodeId != InvalidNodeId;

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    auto* targetNode = nodeTracker->FindNode(targetNodeId);
    if (isPullReplicationJob && !targetNode) {
        return false;
    }

    auto finallyGuard = Finally([&] {
        if (job) {
            return;
        }

        if (targetNode) {
            targetNode->UnrefChunkBeingPulled(chunkId, mediumIndex);
        }
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

    TNodePtrWithReplicaAndMediumIndexList targetReplicas;
    for (auto* node : targetNodes) {
        if (!targetNode || targetNode == node) {
            targetReplicas.emplace_back(node, replicaIndex, targetMediumIndex);
        }
    }

    if (targetReplicas.empty()) {
        return false;
    }

    job = New<TReplicationJob>(
        context->GenerateJobId(),
        GetJobEpoch(chunk),
        sourceNode,
        TChunkPtrWithReplicaIndex(chunk, replicaIndex),
        targetReplicas,
        targetNodeId);
    context->ScheduleJob(job);

    YT_LOG_DEBUG("Replication job scheduled "
        "(JobId: %v, JobEpoch: %v, Address: %v, ChunkId: %v, TargetAddresses: %v, IsPullReplicationJob: %v)",
        job->GetJobId(),
        job->GetJobEpoch(),
        sourceNode->GetDefaultAddress(),
        chunkWithIndex,
        MakeFormattableView(targetNodes, TNodePtrAddressFormatter()),
        isPullReplicationJob);

    if (targetNode) {
        replicasNeeded = 1;
    }

    return std::ssize(targetNodes) == replicasNeeded;
}

bool TChunkReplicator::TryScheduleRemovalJob(
    IJobSchedulingContext* context,
    const TChunkIdWithIndexes& chunkIdWithIndexes,
    TRealChunkLocation* location)
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
    TChunkPtrWithReplicaAndMediumIndex chunkWithIndexes(
        IsObjectAlive(chunk) ? chunk : nullptr,
        chunkIdWithIndexes.ReplicaIndex,
        chunkIdWithIndexes.MediumIndex);
    auto job = New<TRemovalJob>(
        context->GenerateJobId(),
        JobEpochs_[shardIndex],
        context->GetNode(),
        IsObjectAlive(chunk) ? chunk : nullptr,
        chunkIdWithIndexes,
        location ? location->GetUuid() : InvalidChunkLocationUuid);
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
    TChunkPtrWithReplicaAndMediumIndex chunkWithIndexes)
{
    YT_VERIFY(chunkWithIndexes.GetReplicaIndex() == GenericChunkReplicaIndex);

    auto* chunk = chunkWithIndexes.GetPtr();
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

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    int mediumIndex = chunkWithIndexes.GetMediumIndex();
    auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
    if (!IsObjectAlive(medium)) {
        YT_LOG_ALERT(
            "Attempted to schedule repair job for non-existent medium, ignored "
            "(ChunkId: %v, MediumIndex: %v)",
            chunk->GetId(),
            mediumIndex);
        return true;
    }
    if (medium->IsOffshore()) {
        YT_LOG_ALERT(
            "Attempted to schedule repair job for offshore medium, ignored "
            "(ChunkId: %v, MediumIndex: %v, MediumName: %v, MediumType: %v)",
            chunk->GetId(),
            medium->GetIndex(),
            medium->GetName(),
            medium->GetType());
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
        medium->AsDomestic(),
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

    TNodePtrWithReplicaAndMediumIndexList targetReplicas;
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

void TChunkReplicator::ScheduleJobs(EJobType jobType, IJobSchedulingContext* context)
{
    YT_VERIFY(IsMasterJobType(jobType));

    if (!IsReplicatorEnabled()) {
        return;
    }

    switch (jobType) {
        case EJobType::ReplicateChunk:
            ScheduleReplicationJobs(context);
            break;
        case EJobType::RemoveChunk:
            ScheduleRemovalJobs(context);
            break;
        case EJobType::RepairChunk:
            ScheduleRepairJobs(context);
            break;
        default:
            break;
    }
}

void TChunkReplicator::ScheduleReplicationJobs(IJobSchedulingContext* context)
{
    auto* node = context->GetNode();
    const auto& resourceUsage = context->GetNodeResourceUsage();
    const auto& resourceLimits = context->GetNodeResourceLimits();

    int misscheduledReplicationJobs = 0;

    // NB: Beware of chunks larger than the limit; we still need to be able to replicate them one by one.
    auto hasSpareReplicationResources = [&] {
        return
            misscheduledReplicationJobs < GetDynamicConfig()->MaxMisscheduledReplicationJobsPerHeartbeat &&
            resourceUsage.replication_slots() < resourceLimits.replication_slots() &&
            (resourceUsage.replication_slots() == 0 || resourceUsage.replication_data_size() < resourceLimits.replication_data_size());
    };

    const auto& incumbentManager = Bootstrap_->GetIncumbentManager();
    auto incumbentCount = incumbentManager->GetIncumbentCount(EIncumbentType::ChunkReplicator);
    auto hasSparePullReplicationResources = [&] {
        auto maxPullReplicationJobs = GetDynamicConfig()->MaxRunningReplicationJobsPerTargetNode / std::max(incumbentCount, 1);
        return
            // TODO(gritukan): Use some better bounds.
            misscheduledReplicationJobs < GetDynamicConfig()->MaxMisscheduledReplicationJobsPerHeartbeat &&
            std::ssize(node->ChunksBeingPulled()) < maxPullReplicationJobs;
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

            if (!chunk || !chunk->IsRefreshActual()) {
                queue.erase(jt);
                ++misscheduledReplicationJobs;
                continue;
            }

            for (const auto& replica : chunk->StoredReplicas()) {
                auto* pushNode = GetChunkLocationNode(replica);
                for (int mediumIndex = 0; mediumIndex < std::ssize(mediumIndexSet); ++mediumIndex) {
                    if (mediumIndexSet.test(mediumIndex)) {
                        if (pushNode->GetTargetReplicationNodeId(chunk->GetId(), mediumIndex) != InvalidNodeId) {
                            // Replication is already planned with another node as a destination.
                            continue;
                        }

                        if (desiredReplica.ReplicaIndex != replica.GetReplicaIndex()) {
                            continue;
                        }

                        TChunkPtrWithReplicaIndex chunkWithIndex(
                            chunk,
                            replica.GetReplicaIndex());
                        pushNode->AddToChunkPushReplicationQueue(chunkWithIndex, mediumIndex, priority);
                        pushNode->AddTargetReplicationNodeId(chunk->GetId(), mediumIndex, node);

                        node->RefChunkBeingPulled(chunk->GetId(), mediumIndex);
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
            auto chunkWithIndex = jt->first;
            auto* chunk = chunkWithIndex.GetPtr();
            auto& mediumIndexSet = jt->second;
            auto chunkId = chunk->GetId();

            if (!chunk->IsRefreshActual()) {
                // NB: Call below removes chunk from #queue.
                RemoveFromChunkReplicationQueues(node, chunkWithIndex);
                ++misscheduledReplicationJobs;
                continue;
            }

            for (int mediumIndex = 0; mediumIndex < std::ssize(mediumIndexSet); ++mediumIndex) {
                if (mediumIndexSet.test(mediumIndex)) {
                    auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                    if (!IsObjectAlive(medium)) {
                        YT_LOG_ALERT(
                            "Attempted to schedule replication job for non-existent medium, ignored "
                            "(ChunkId: %v, MediumIndex: %v)",
                            chunk->GetId(),
                            mediumIndex);
                        ++misscheduledReplicationJobs;
                        // Something bad happened, let's try to forget it.
                        mediumIndexSet.reset(mediumIndex);
                        ScheduleChunkRefresh(chunk);

                        continue;
                    }
                    if (medium->IsOffshore()) {
                        YT_LOG_ALERT(
                            "Attempted to schedule replication job for offshore medium, ignored "
                            "(ChunkId: %v, MediumIndex: %v, MediumName: %v, MediumType: %v)",
                            chunk->GetId(),
                            medium->GetIndex(),
                            medium->GetName(),
                            medium->GetType());
                        ++misscheduledReplicationJobs;
                        // Something bad happened, let's try to forget it.
                        mediumIndexSet.reset(mediumIndex);
                        ScheduleChunkRefresh(chunk);

                        continue;
                    }

                    auto nodeId = node->GetTargetReplicationNodeId(chunkId, mediumIndex);
                    node->RemoveTargetReplicationNodeId(chunkId, mediumIndex);

                    if (TryScheduleReplicationJob(
                        context,
                        chunkWithIndex,
                        medium->AsDomestic(),
                        nodeId))
                    {
                        mediumIndexSet.reset(mediumIndex);
                    } else {
                        ++misscheduledReplicationJobs;
                        if (nodeId != InvalidNodeId) {
                            mediumIndexSet.reset(mediumIndex);
                            // Move all CRP-enabled chunks with mischeduled jobs back to pull queue.
                            ScheduleChunkRefresh(chunk);
                        }
                    }
                }
            }

            if (mediumIndexSet.none()) {
                queue.erase(jt);
            }
        }
    }

    MisscheduledJobs_[EJobType::ReplicateChunk] += misscheduledReplicationJobs;
}

void TChunkReplicator::ScheduleRemovalJobs(IJobSchedulingContext* context)
{
    if (TInstant::Now() < LastActiveShardSetUpdateTime_ + GetDynamicConfig()->RemovalJobScheduleDelay) {
        return;
    }

    auto* node = context->GetNode();
    const auto& resourceUsage = context->GetNodeResourceUsage();
    const auto& resourceLimits = context->GetNodeResourceLimits();

    int misscheduledRemovalJobs = 0;

    auto hasSpareRemovalResources = [&] {
        return
            misscheduledRemovalJobs < GetDynamicConfig()->MaxMisscheduledRemovalJobsPerHeartbeat &&
            resourceUsage.removal_slots() < resourceLimits.removal_slots();
    };

    const auto& chunkManager = Bootstrap_->GetChunkManager();

    // Schedule removal jobs.
    const auto& nodeJobs = JobRegistry_->GetNodeJobs(node->GetDefaultAddress());

    THashSet<TChunkIdWithIndexes> chunksBeingRemoved;
    for (const auto& job : nodeJobs) {
        if (job->GetType() != EJobType::RemoveChunk) {
            continue;
        }
        chunksBeingRemoved.insert(job->GetChunkIdWithIndexes());
    }

    // TODO(gritukan): Use sharded destroyed replica sets here.
    if (GetActiveShardCount() > 0) {
        struct TLocationInfo
        {
            TChunkLocation* Location;
            TChunkLocation::TDestroyedReplicasIterator ReplicaIterator;
            bool Active = true;
        };
        TCompactVector<TLocationInfo, TypicalChunkLocationCount> locations;
        for (auto* location : node->ChunkLocations()) {
            if (!location->DestroyedReplicas().empty()) {
                locations.push_back({location, location->GetDestroyedReplicasIterator()});
            }
        }
        int activeLocationCount = std::ssize(locations);

        while (activeLocationCount > 0 && hasSpareRemovalResources()) {
            for (auto& [location, replicaIterator, active] : locations) {
                if (activeLocationCount <= 0 || !hasSpareRemovalResources()) {
                    break;
                }

                if (!active) {
                    continue;
                }

                auto mediumIndex = location->GetEffectiveMediumIndex();
                TChunkIdWithIndexes replica(*replicaIterator, mediumIndex);

                if (!replicaIterator.Advance()) {
                    active = false;
                    --activeLocationCount;
                }

                if (chunksBeingRemoved.contains(replica)) {
                    continue;
                }

                if (!ShouldProcessChunk(replica.Id)) {
                    location->SetDestroyedReplicasIterator(replicaIterator);
                    ++misscheduledRemovalJobs;
                } else if (TryScheduleRemovalJob(context, replica, location->IsImaginary() ? nullptr : location->AsReal())) {
                    location->SetDestroyedReplicasIterator(replicaIterator);
                } else {
                    ++misscheduledRemovalJobs;
                }
            }
        }
    }

    {
        TCompactVector<
            std::pair<TChunkLocation*, TChunkLocation::TChunkRemovalQueue::iterator>,
            TypicalChunkLocationCount> locations;
        for (auto* location : node->ChunkLocations()) {
            auto& queue = location->ChunkRemovalQueue();
            if (!queue.empty()) {
                locations.emplace_back(location, queue.begin());
            }
        }
        int activeLocationCount = std::ssize(locations);

        while (activeLocationCount > 0 && hasSpareRemovalResources()) {
            for (auto& [location, replicaIterator] : locations) {
                if (activeLocationCount <= 0 || !hasSpareRemovalResources()) {
                    break;
                }

                auto& queue = location->ChunkRemovalQueue();
                if (replicaIterator == queue.end()) {
                    continue;
                }

                auto replica = replicaIterator;
                if (++replicaIterator == queue.end()) {
                    --activeLocationCount;
                }

                auto* chunk = chunkManager->FindChunk(replica->Id);
                auto mediumIndex = location->GetEffectiveMediumIndex();
                TChunkIdWithIndexes chunkIdWithIndexes(*replica, mediumIndex);
                if (!ShouldProcessChunk(replica->Id) || (IsObjectAlive(chunk) && !chunk->IsRefreshActual())) {
                    queue.erase(replica);
                    ++misscheduledRemovalJobs;
                } else if (chunksBeingRemoved.contains(chunkIdWithIndexes)) {
                    YT_LOG_ALERT(
                        "Trying to schedule a removal job for a chunk that is already being removed (ChunkId: %v)",
                        chunkIdWithIndexes);
                } else if (TryScheduleRemovalJob(context, chunkIdWithIndexes, location->IsImaginary() ? nullptr : location->AsReal())) {
                    queue.erase(replica);
                } else {
                    ++misscheduledRemovalJobs;
                }
            }
        }
    }


    MisscheduledJobs_[EJobType::RemoveChunk] += misscheduledRemovalJobs;
}

void TChunkReplicator::ScheduleRepairJobs(IJobSchedulingContext* context)
{
    auto* node = context->GetNode();
    const auto& resourceUsage = context->GetNodeResourceUsage();
    const auto& resourceLimits = context->GetNodeResourceLimits();

    int misscheduledRepairJobs = 0;

    // NB: Beware of chunks larger than the limit; we still need to be able to repair them one by one.
    auto hasSpareRepairResources = [&] {
        return
            misscheduledRepairJobs < GetDynamicConfig()->MaxMisscheduledRepairJobsPerHeartbeat &&
            resourceUsage.repair_slots() < resourceLimits.repair_slots() &&
            (resourceUsage.repair_slots() == 0 || resourceUsage.repair_data_size() < resourceLimits.repair_data_size());
    };

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
            auto* chunk = chunkIt->GetPtr();

            auto removeFromQueue = [&] {
                chunk->SetRepairQueueIterator(mediumIndex, queue, TChunkRepairQueueIterator());
                chunkRepairQueue.erase(chunkIt);
            };

            // NB: Repair queues are not cleared when shard processing is stopped,
            // so we have to handle chunks replicator should not process.
            TChunkPtrWithReplicaAndMediumIndex chunkWithIndexes(chunk, GenericChunkReplicaIndex, mediumIndex);
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

    MisscheduledJobs_[EJobType::RepairChunk] += misscheduledRepairJobs;
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

    auto durabilityRequired = IsDurabilityRequired(chunk);

    const auto& chunkManager = Bootstrap_->GetChunkManager();

    for (auto entry : replication) {
        auto mediumIndex = entry.GetMediumIndex();
        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        YT_VERIFY(IsObjectAlive(medium));

        auto& statistics = allMediaStatistics.PerMediumStatistics[mediumIndex];
        if (statistics.Status == EChunkStatus::None) {
            continue;
        }

        if (Any(statistics.Status & EChunkStatus::Overreplicated)) {
            OverreplicatedChunks_.insert(chunk);
        }

        if (Any(statistics.Status & EChunkStatus::UnexpectedOverreplicated)) {
            UnexpectedOverreplicatedChunks_.insert(chunk);
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
                for (auto locationWithIndexes : statistics.DecommissionedRemovalReplicas) {
                    auto* location = locationWithIndexes.GetPtr();
                    auto* node = location->GetNode();
                    if (!node->ReportedDataNodeHeartbeat()) {
                        continue;
                    }

                    TChunkIdWithIndexes chunkIdWithIndexes(
                        chunk->GetId(),
                        locationWithIndexes.GetReplicaIndex(),
                        location->GetEffectiveMediumIndex());
                    location->AddToChunkRemovalQueue(chunkIdWithIndexes);
                }

                for (int replicaIndex : statistics.BalancingRemovalIndexes) {
                    TChunkPtrWithReplicaAndMediumIndex chunkWithIndexes(chunk, replicaIndex, mediumIndex);
                    auto* targetLocation = ChunkPlacement_->GetRemovalTarget(chunkWithIndexes);
                    if (!targetLocation) {
                        continue;
                    }

                    TChunkIdWithIndexes chunkIdWithIndexes(chunk->GetId(), replicaIndex, mediumIndex);
                    targetLocation->AddToChunkRemovalQueue(chunkIdWithIndexes);
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

                            node->AddToChunkPullReplicationQueue(
                                {chunk, replica.GetReplicaIndex()},
                                mediumIndex,
                                priority);
                        }
                    } else {
                        for (auto replica : chunk->StoredReplicas()) {
                            if (chunk->IsJournal() && replica.GetReplicaState() != EChunkReplicaState::Sealed) {
                                continue;
                            }

                            if (replica.GetReplicaIndex() != replicaIndex) {
                                continue;
                            }

                            // If chunk is lost on some media, don't match dst medium with
                            // src medium: we want to be able to do cross-medium replication.
                            bool mediumMatches =
                                Any(allMediaStatistics.Status & ECrossMediumChunkStatus::MediumWiseLost) ||
                                mediumIndex == replica.GetPtr()->GetEffectiveMediumIndex();
                            if (!mediumMatches) {
                                continue;
                            }

                            auto* node = replica.GetPtr()->GetNode();
                            if (!node->ReportedDataNodeHeartbeat()) {
                                continue;
                            }

                            TChunkPtrWithReplicaIndex chunkWithIndex(chunk, replica.GetReplicaIndex());
                            node->AddToChunkPushReplicationQueue(chunkWithIndex, mediumIndex, priority);
                        }
                    }
                }
            }

            if (None(statistics.Status & EChunkStatus::Lost) && chunk->IsSealed()) {
                TChunkPtrWithMediumIndex chunkWithIndex(chunk, mediumIndex);
                if (Any(statistics.Status & (EChunkStatus::DataMissing | EChunkStatus::ParityMissing))) {
                    AddToChunkRepairQueue(chunkWithIndex, EChunkRepairQueue::Missing);
                } else if (Any(statistics.Status & (EChunkStatus::DataDecommissioned | EChunkStatus::ParityDecommissioned))) {
                    AddToChunkRepairQueue(chunkWithIndex, EChunkRepairQueue::Decommissioned);
                }
            }
        }
    }

    if (Any(allMediaStatistics.Status & ECrossMediumChunkStatus::Sealed)) {
        YT_ASSERT(chunk->IsJournal());
        for (auto replica : chunk->StoredReplicas()) {
            if (replica.GetReplicaState() != EChunkReplicaState::Unsealed) {
                continue;
            }

            auto* location = replica.GetPtr();
            if (!location->GetNode()->ReportedDataNodeHeartbeat()) {
                continue;
            }

            location->AddToChunkSealQueue({chunk, replica.GetReplicaIndex()});
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
    UnexpectedOverreplicatedChunks_.erase(chunk);
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
        auto* location = replica.GetPtr();
        auto* node = location->GetNode();
        int mediumIndex = location->GetEffectiveMediumIndex();

        RemoveFromChunkReplicationQueues(node, {chunk, replica.GetReplicaIndex()});

        TChunkIdWithIndexes chunkIdWithIndexes(chunk->GetId(), replica.GetReplicaIndex(), mediumIndex);
        location->RemoveFromChunkRemovalQueue(chunkIdWithIndexes);
    }

    RemoveFromChunkRepairQueues(chunk);
}

void TChunkReplicator::RemoveChunkFromQueuesOnDestroy(TChunk* chunk)
{
    // Remove chunk from replication and seal queues.
    for (auto replica : chunk->StoredReplicas()) {
        auto* location = replica.GetPtr();
        auto* node = location->GetNode();
        TChunkPtrWithReplicaIndex chunkWithIndex(chunk, replica.GetReplicaIndex());
        // NB: Keep existing removal requests to workaround the following scenario:
        // 1) the last strong reference to a chunk is released while some ephemeral references
        //    remain; the chunk becomes a zombie;
        // 2) a node sends a heartbeat reporting addition of the chunk;
        // 3) master receives the heartbeat and puts the chunk into the removal queue
        //    without (sic!) registering a replica;
        // 4) the last ephemeral reference is dropped, the chunk is being removed;
        //    at this point we must preserve its removal request in the queue.
        RemoveFromChunkReplicationQueues(node, chunkWithIndex);
        location->RemoveFromChunkSealQueue(chunkWithIndex);
    }

    RemoveFromChunkRepairQueues(chunk);
}

bool TChunkReplicator::IsReplicaDecommissioned(TChunkLocation* replica)
{
    return replica->GetNode()->IsDecommissioned();
}

TChunkReplication TChunkReplicator::GetChunkAggregatedReplication(const TChunk* chunk) const
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto result = chunk->GetAggregatedReplication(GetChunkRequisitionRegistry());
    for (auto& entry : result) {
        YT_VERIFY(entry.Policy());

        auto* medium = chunkManager->FindMediumByIndex(entry.GetMediumIndex());
        YT_VERIFY(IsObjectAlive(medium));
        // For now, all offshore media have replication parameters in settings, so
        // from replicator's point of view chunk has single replica.
        auto cap = medium->IsDomestic()
            ? medium->AsDomestic()->Config()->MaxReplicationFactor
            : 1;

        entry.Policy().SetReplicationFactor(std::min(cap, entry.Policy().GetReplicationFactor()));
    }

    // A chunk may happen to have replicas stored on a medium it's not supposed
    // to have replicas on. (This is common when chunks are being relocated from
    // one medium to another.) Add corresponding entries to the aggregated
    // replication so that such media aren't overlooked.
    for (auto replica : chunk->StoredReplicas()) {
        auto mediumIndex = replica.GetPtr()->GetEffectiveMediumIndex();
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
    // For now, all offshore media have replication parameters in settings, so
    // from replicator's point of view chunk has single replica.
    auto cap = medium->IsDomestic()
        ? medium->AsDomestic()->Config()->MaxReplicationFactor
        : 1;

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

    for (auto* location : node->ChunkLocations()) {
        const auto* medium = chunkManager->FindMediumByIndex(location->GetEffectiveMediumIndex());
        if (!medium) {
            continue;
        }

        for (auto replica : location->Replicas()) {
            ScheduleChunkRefresh(replica.GetPtr());
        }
    }
}

void TChunkReplicator::ScheduleGlobalChunkRefresh()
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
        if (IsShardActive(shardIndex)) {
            BlobRefreshScanner_->ScheduleGlobalScan(chunkManager->GetGlobalBlobChunkScanDescriptor(shardIndex));
            JournalRefreshScanner_->ScheduleGlobalScan(chunkManager->GetGlobalJournalChunkScanDescriptor(shardIndex));
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
    YT_ASSERT(chunk);
    return IsShardActive(chunk->GetShardIndex());
}

bool TChunkReplicator::ShouldProcessChunk(TChunkId chunkId)
{
    return IsShardActive(GetChunkShardIndex(chunkId));
}

TJobEpoch TChunkReplicator::GetJobEpoch(TChunk* chunk) const
{
    return JobEpochs_[chunk->GetShardIndex()];
}

bool TChunkReplicator::IsDurabilityRequired(TChunk* chunk) const
{
    if (chunk->GetHistoricallyNonVital()) {
        return false;
    }

    if (chunk->IsErasure()) {
        return true;
    }

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto replication = GetChunkAggregatedReplication(chunk);
    return replication.IsDurabilityRequired(chunkManager);
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
    auto proxy = CreateObjectServiceReadProxy(
        Bootstrap_->GetRootClient(),
        NApi::EMasterChannelKind::Leader);

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
        TChunkLocation* location;
        if (node->UseImaginaryChunkLocations()) {
            location = node->GetImaginaryChunkLocation(replica.MediumIndex);
        } else {
            auto* removalJob = static_cast<TRemovalJob*>(unsucceededJob.Get());
            auto locationUuid = removalJob->ChunkLocationUuid();
            const auto& dataNodeTracker = Bootstrap_->GetDataNodeTracker();
            location = dataNodeTracker->FindChunkLocationByUuid(locationUuid);
            if (!location) {
                YT_LOG_ALERT(
                    "Cannot reschedule chunk removal job; location not found "
                    "(Chunk: %v, LocationUuid: %v)",
                    replica,
                    locationUuid);
                return;
            }

            if (location->GetEffectiveMediumIndex() != replica.MediumIndex) {
                YT_LOG_DEBUG(
                    "Chunk removal job was not rescheduled because location's medium has been changed "
                    "(Chunk: %v, LocationUuid: %v, LocationMediumIndex: %v)",
                    replica,
                    locationUuid,
                    location->GetEffectiveMediumIndex());
                return;
            }
        }
        location->AddToChunkRemovalQueue(replica);
    }
}

void TChunkReplicator::OnProfiling(TSensorBuffer* buffer, TSensorBuffer* crpBuffer)
{
    buffer->AddGauge("/blob_refresh_queue_size", BlobRefreshScanner_->GetQueueSize());
    buffer->AddGauge("/blob_requisition_update_queue_size", BlobRequisitionUpdateScanner_->GetQueueSize());
    buffer->AddGauge("/journal_refresh_queue_size", JournalRefreshScanner_->GetQueueSize());
    buffer->AddGauge("/journal_requisition_update_queue_size", JournalRequisitionUpdateScanner_->GetQueueSize());

    buffer->AddGauge("/lost_chunk_count", LostChunks_.size());
    buffer->AddGauge("/lost_vital_chunk_count", LostVitalChunks_.size());
    buffer->AddGauge("/overreplicated_chunk_count", OverreplicatedChunks_.size());
    buffer->AddGauge("/unexpected_overreplicated_chunk_count", UnexpectedOverreplicatedChunks_.size());
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
        for (auto [_, node] : Bootstrap_->GetNodeTracker()->Nodes()) {
            TWithTagGuard tagGuard(crpBuffer, "node_address", node->GetDefaultAddress());

            i64 pullReplicationQueueSize = 0;
            for (const auto& queue : node->ChunkPullReplicationQueues()) {
                pullReplicationQueueSize += std::ssize(queue);
            }
            crpBuffer->AddGauge("/pull_replication_queue_size", pullReplicationQueueSize);
            crpBuffer->AddGauge("/crp_chunks_being_pulled_count", node->ChunksBeingPulled().size());
            crpBuffer->AddGauge("/crp_push_replication_target_node_id_count", node->PushReplicationTargetNodeIds().size());

            size_t destroyedReplicasCount = node->ComputeTotalDestroyedReplicaCount();
            size_t removalQueueSize = node->ComputeTotalChunkRemovalQueuesSize();
            crpBuffer->AddGauge("/destroyed_replicas_size", destroyedReplicasCount);
            crpBuffer->AddGauge("/removal_queue_size", removalQueueSize);
        }

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

void TChunkReplicator::RemoveChunkFromPullReplicationSet(const TJobPtr& job)
{
    if (job->GetType() != EJobType::ReplicateChunk) {
        return;
    }
    auto replicationJob = StaticPointerCast<TReplicationJob>(job);

    const auto& chunkIdWithIndexes = job->GetChunkIdWithIndexes();
    auto chunkId = chunkIdWithIndexes.Id;

    const auto& targetReplicas = replicationJob->TargetReplicas();
    auto targetMediumIndex = targetReplicas[0].GetMediumIndex();

    UnrefChunkBeingPulled(replicationJob->GetTargetNodeId(), chunkId, targetMediumIndex);
}

void TChunkReplicator::OnJobCompleted(const TJobPtr& job)
{
    RemoveChunkFromPullReplicationSet(job);
}

void TChunkReplicator::OnJobAborted(const TJobPtr& job)
{
    RemoveChunkFromPullReplicationSet(job);
    TryRescheduleChunkRemoval(job);
}

void TChunkReplicator::OnJobFailed(const TJobPtr& job)
{
    RemoveChunkFromPullReplicationSet(job);
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
            if (auto* account = owningNode->Account().Get()) {
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

void TChunkReplicator::AddToChunkRepairQueue(TChunkPtrWithMediumIndex chunkWithIndex, EChunkRepairQueue queue)
{
    auto* chunk = chunkWithIndex.GetPtr();
    int mediumIndex = chunkWithIndex.GetMediumIndex();
    YT_VERIFY(chunk->GetRepairQueueIterator(mediumIndex, queue) == TChunkRepairQueueIterator());
    auto& chunkRepairQueue = ChunkRepairQueue(mediumIndex, queue);
    auto it = chunkRepairQueue.insert(chunkRepairQueue.end(), chunkWithIndex);
    chunk->SetRepairQueueIterator(mediumIndex, queue, it);
}

void TChunkReplicator::RemoveFromChunkRepairQueues(TChunk* chunk)
{
    if (!chunk->IsErasure()) {
        return;
    }

    // NB: when a chunk requisition (and replication) changes, there's a delay
    // before the subsequent refresh. Which means that the requisition and
    // replication/repair/etc. queues may be inconsistent. Do not rely on the
    // requisition when dealing with replicator queues!
    for (auto queue : TEnumTraits<EChunkRepairQueue>::GetDomainValues()) {
        auto queueIterators = *chunk->SelectRepairQueueIteratorMap(queue);
        for (auto [mediumIndex, repairIt] : queueIterators) {
            if (repairIt == TChunkRepairQueueIterator()) {
                continue;
            }
            auto& repairQueue = ChunkRepairQueue(mediumIndex, queue);
            repairQueue.erase(repairIt);
            // Just to be safe.
            chunk->SetRepairQueueIterator(mediumIndex, queue, TChunkRepairQueueIterator());
        }
    }
}

void TChunkReplicator::TouchChunkInRepairQueues(TChunk* chunk)
{
    if (!chunk->IsErasure()) {
        return;
    }

    // NB: see RemoveFromChunkRepairQueues for the comment on why the chunk's
    // requisition should not be used here.
    for (auto queue : TEnumTraits<EChunkRepairQueue>::GetDomainValues()) {
        auto queueIterators = *chunk->SelectRepairQueueIteratorMap(queue);
        for (auto [mediumIndex, repairIt] : queueIterators) {
            if (repairIt == TChunkRepairQueueIterator()) {
                continue;
            }
            auto& repairQueue = ChunkRepairQueue(mediumIndex, queue);
            repairQueue.erase(repairIt);
            TChunkPtrWithMediumIndex chunkWithIndex(chunk, mediumIndex);
            auto newRepairIt = repairQueue.insert(repairQueue.begin(), chunkWithIndex);
            chunk->SetRepairQueueIterator(mediumIndex, queue, newRepairIt);
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
    auto mutation = chunkManager->CreateRegisterChunkEndorsementsMutation(req);
    // NB: This code can be executed either on leader or follower.
    mutation->SetAllowLeaderForwarding(true);
    auto invoker = Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkRefresher);
    mutation->CommitAndLog(Logger)
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TMutationResponse>& error) {
            if (!error.IsOK()) {
                YT_LOG_WARNING(error,
                    "Failed to commit chunk endorsment registration mutation; "
                    "scheduling global refresh");
                ScheduleGlobalChunkRefresh();
            }
        }).AsyncVia(invoker));
}

const std::unique_ptr<TChunkScanner>& TChunkReplicator::GetChunkRefreshScanner(TChunk* chunk) const
{
    return chunk->IsJournal() ? JournalRefreshScanner_ : BlobRefreshScanner_;
}

const std::unique_ptr<TChunkScanner>& TChunkReplicator::GetChunkRequisitionUpdateScanner(TChunk* chunk) const
{
    return chunk->IsJournal() ? JournalRequisitionUpdateScanner_ : BlobRequisitionUpdateScanner_;
}

TChunkRequisitionRegistry* TChunkReplicator::GetChunkRequisitionRegistry() const
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
    if (EnabledCheckExecutor_) {
        EnabledCheckExecutor_->SetPeriod(GetDynamicConfig()->ReplicatorEnabledCheckPeriod);
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
    BlobRefreshScanner_->Start(chunkManager->GetGlobalBlobChunkScanDescriptor(shardIndex));
    JournalRefreshScanner_->Start(chunkManager->GetGlobalJournalChunkScanDescriptor(shardIndex));

    auto& jobEpoch = JobEpochs_[shardIndex];
    YT_VERIFY(jobEpoch == InvalidJobEpoch);
    jobEpoch = JobRegistry_->StartEpoch();
    SetRefreshEpoch(shardIndex, jobEpoch);

    LastActiveShardSetUpdateTime_ = TInstant::Now();

    YT_LOG_INFO("Chunk refresh started (ShardIndex: %v, JobEpoch: %v)",
        shardIndex,
        jobEpoch);
}

void TChunkReplicator::StopRefresh(int shardIndex)
{
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
    clearChunkSetShard(OverreplicatedChunks_);
    clearChunkSetShard(UnexpectedOverreplicatedChunks_);
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
    auto previousJobEpoch = std::exchange(jobEpoch, InvalidJobEpoch);
    SetRefreshEpoch(shardIndex, jobEpoch);

    LastActiveShardSetUpdateTime_ = TInstant::Now();

    YT_LOG_INFO("Chunk refresh stopped (ShardIndex: %v, JobEpoch: %v)",
        shardIndex,
        previousJobEpoch);
}

void TChunkReplicator::StartRequisitionUpdate()
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
        BlobRequisitionUpdateScanner_->Start(chunkManager->GetGlobalBlobChunkScanDescriptor(shardIndex));
        JournalRequisitionUpdateScanner_->Start(chunkManager->GetGlobalJournalChunkScanDescriptor(shardIndex));
    }

    YT_VERIFY(!std::exchange(RequisitionUpdateRunning_, true));

    YT_LOG_INFO("Chunk requisition update started");
}

void TChunkReplicator::StopRequisitionUpdate()
{
    if (!std::exchange(RequisitionUpdateRunning_, false)) {
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
