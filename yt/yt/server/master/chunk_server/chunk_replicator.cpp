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
#include "incumbency_epoch.h"

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

#include <yt/yt/server/lib/chunk_server/proto/job.pb.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/sequoia_client/records/location_replicas.record.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/small_containers/compact_queue.h>
#include <library/cpp/yt/small_containers/compact_vector.h>

#include <array>

namespace NYT::NChunkServer {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NHydra;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NChunkServer::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NNodeTrackerServer;
using namespace NIncumbentClient;
using namespace NObjectServer;
using namespace NChunkServer::NProto;
using namespace NCellMaster;
using namespace NTransactionClient;

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
        const auto& chunkManager = bootstrap->GetChunkManager();
        // This is context switch, but Chunk_ is ephemeral pointer.
        auto replicasOrError = chunkManager->GetChunkReplicas(Chunk_);
        if (!replicasOrError.IsOK()) {
            return false;
        }
        const auto& chunkReplicas = replicasOrError.Value();
        for (auto replica : chunkReplicas) {
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

    bool FillJobSpec(TBootstrap* bootstrap, TJobSpec* jobSpec) const override
    {
        if (!IsObjectAlive(Chunk_)) {
            return false;
        }

        auto* jobSpecExt = jobSpec->MutableExtension(TRepairChunkJobSpecExt::repair_chunk_job_spec_ext);
        jobSpecExt->set_erasure_codec(ToProto<int>(Chunk_->GetErasureCodec()));
        ToProto(jobSpecExt->mutable_chunk_id(), Chunk_->GetId());
        jobSpecExt->set_decommission(Decommission_);

        if (Chunk_->IsJournal()) {
            YT_VERIFY(Chunk_->IsSealed());
            jobSpecExt->set_row_count(Chunk_->GetPhysicalSealedRowCount());
        }

        NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());

        const auto& chunkManager = bootstrap->GetChunkManager();
        // This is context switch, but Chunk_ is ephemeral pointer.
        auto replicasOrError = chunkManager->GetChunkReplicas(Chunk_);
        if (!replicasOrError.IsOK()) {
            return false;
        }
        const auto& sourceReplicas = replicasOrError.Value();
        builder.Add(sourceReplicas);
        ToProto(jobSpecExt->mutable_source_replicas(), sourceReplicas);
        ToProto(jobSpecExt->mutable_legacy_source_replicas(), sourceReplicas);

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
    , BlobRefreshScanner_(std::make_unique<TChunkRefreshScanner>(
        EChunkScanKind::Refresh,
        /*journal*/ false))
    , JournalRefreshScanner_(std::make_unique<TChunkRefreshScanner>(
        EChunkScanKind::Refresh,
        /*journal*/ true))
    , BlobRequisitionUpdateScanner_(std::make_unique<TChunkScanner>(
        EChunkScanKind::RequisitionUpdate,
        /*journal*/ false))
    , JournalRequisitionUpdateScanner_(std::make_unique<TChunkScanner>(
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
        SetIncumbencyEpoch(shardIndex, InvalidIncumbencyEpoch);
    }

    LocationShards_.reserve(TypicalChunkLocationCount * ChunkShardCount);

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

    ScheduleChunkRequisitionUpdatesExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkRequisitionUpdater),
        BIND(&TChunkReplicator::OnScheduleChunkRequisitionUpdatesFlush, MakeWeak(this)),
        GetDynamicConfig()->ScheduledChunkRequisitionUpdatesFlushPeriod);
    ScheduleChunkRequisitionUpdatesExecutor_->Start();

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

    // Just in case.
    Enabled_ = false;
}

void TChunkReplicator::OnEpochFinished()
{
    LastPerNodeProfilingTime_ = TInstant::Zero();

    if (RefreshExecutor_) {
        YT_UNUSED_FUTURE(RefreshExecutor_->Stop());
        RefreshExecutor_.Reset();
    }

    if (ScheduleChunkRequisitionUpdatesExecutor_) {
        YT_UNUSED_FUTURE(ScheduleChunkRequisitionUpdatesExecutor_->Stop());
        ScheduleChunkRequisitionUpdatesExecutor_.Reset();
    }

    if (RequisitionUpdateExecutor_) {
        YT_UNUSED_FUTURE(RequisitionUpdateExecutor_->Stop());
        RequisitionUpdateExecutor_.Reset();
    }

    if (FinishedRequisitionTraverseFlushExecutor_) {
        YT_UNUSED_FUTURE(FinishedRequisitionTraverseFlushExecutor_->Stop());
        FinishedRequisitionTraverseFlushExecutor_.Reset();
    }

    if (EnabledCheckExecutor_) {
        YT_UNUSED_FUTURE(EnabledCheckExecutor_->Stop());
        EnabledCheckExecutor_.Reset();
    }

    ClearChunkRequisitionCache();
    TmpRequisitionRegistry_.Clear();
    ChunkListIdsWithFinishedRequisitionTraverse_.clear();
    ChunkIdsAwaitingRequisitionUpdateScheduling_.clear();

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

void TChunkReplicator::OnIncumbencyStarted(int shardIndex)
{
    TShardedIncumbentBase::OnIncumbencyStarted(shardIndex);

    StartRefreshes(shardIndex);
    StartRequisitionUpdates(shardIndex);

    auto& jobEpoch = JobEpochs_[shardIndex];
    YT_VERIFY(jobEpoch == InvalidJobEpoch);
    jobEpoch = JobRegistry_->StartEpoch();
    SetIncumbencyEpoch(shardIndex, jobEpoch);

    LastActiveShardSetUpdateTime_ = TInstant::Now();

    YT_LOG_INFO("Incumbency started (ShardIndex: %v, JobEpoch: %v)",
        shardIndex,
        jobEpoch);
}

void TChunkReplicator::OnIncumbencyFinished(int shardIndex)
{
    TShardedIncumbentBase::OnIncumbencyFinished(shardIndex);

    StopRefreshes(shardIndex);
    StopRequisitionUpdates(shardIndex);

    auto& jobEpoch = JobEpochs_[shardIndex];
    YT_VERIFY(jobEpoch != InvalidJobEpoch);
    JobRegistry_->OnEpochFinished(jobEpoch);
    auto previousJobEpoch = std::exchange(jobEpoch, InvalidJobEpoch);
    SetIncumbencyEpoch(shardIndex, jobEpoch);

    LastActiveShardSetUpdateTime_ = TInstant::Now();

    YT_LOG_INFO("Incumbency finished (ShardIndex: %v, JobEpoch: %v)",
        shardIndex,
        previousJobEpoch);
}

void TChunkReplicator::TouchChunk(TChunk* chunk)
{
    // Fast path.
    if (!ShouldProcessChunk(chunk)) {
        return;
    }

    TouchChunkInRepairQueues(chunk);
}

TCompactMediumMap<EChunkStatus> TChunkReplicator::ComputeChunkStatuses(
    TChunk* chunk,
    const TChunkLocationPtrWithReplicaInfoList& replicas)
{
    TCompactMediumMap<EChunkStatus> result;

    auto statistics = ComputeChunkStatistics(chunk, replicas);

    for (const auto& [mediumIndex, mediumStatistics] : statistics.PerMediumStatistics) {
        result[mediumIndex] = mediumStatistics.Status;
    }

    return result;
}

ECrossMediumChunkStatus TChunkReplicator::ComputeCrossMediumChunkStatus(
    TChunk* chunk,
    const TChunkLocationPtrWithReplicaInfoList& replicas)
{
    return ComputeChunkStatistics(chunk, replicas).Status;
}

TChunkReplicator::TChunkStatistics TChunkReplicator::ComputeChunkStatistics(
    const TChunk* chunk,
    const TChunkLocationPtrWithReplicaInfoList& replicas)
{
    auto result = chunk->IsErasure()
        ? ComputeErasureChunkStatistics(chunk, replicas)
        : ComputeRegularChunkStatistics(chunk, replicas);

    if (chunk->IsJournal() && chunk->IsSealed()) {
        result.Status |= ECrossMediumChunkStatus::Sealed;
    }

    return result;
}

TChunkReplicator::TChunkStatistics TChunkReplicator::ComputeErasureChunkStatistics(
    const TChunk* chunk,
    const TChunkLocationPtrWithReplicaInfoList& replicas)
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

    TCompactMediumMap<int> replicaCount;
    TCompactMediumMap<int> decommissionedReplicaCount;
    TCompactMediumMap<int> temporarilyUnavailableReplicaCount;

    NErasure::TPartIndexSet replicaIndexes;

    bool totallySealed = chunk->IsSealed();

    auto mark = TNode::GenerateVisitMark();
    auto chunkReplication = GetChunkAggregatedReplication(chunk, replicas);
    for (const auto& entry : chunkReplication) {
        auto mediumIndex = entry.GetMediumIndex();
        unsafelyPlacedSealedReplicas[mediumIndex] = {};
        replicaCount[mediumIndex] = 0;
        decommissionedReplicaCount[mediumIndex] = 0;
    }

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    for (auto replica : replicas) {
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
            ++decommissionedReplicaCount[mediumIndex];
        } else if (IsReplicaOnPendingRestartNode(replica.GetPtr())) {
            ++mediumStatistics.TemporarilyUnavailableReplicaCount[replicaIndex];
            ++temporarilyUnavailableReplicaCount[mediumIndex];
        } else {
            ++mediumStatistics.ReplicaCount[replicaIndex];
            ++replicaCount[mediumIndex];
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
            replicaCount[mediumIndex] == 0 &&
            decommissionedReplicaCount[mediumIndex] == 0 &&
            temporarilyUnavailableReplicaCount[mediumIndex] == 0)
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

TCompactMediumMap<TNodeList> TChunkReplicator::GetChunkConsistentPlacementNodes(
    const TChunk* chunk,
    const TChunkLocationPtrWithReplicaInfoList& replicas)
{
    if (!chunk->HasConsistentReplicaPlacementHash()) {
        return {};
    }

    if (!IsConsistentChunkPlacementEnabled()) {
        return {};
    }

    const auto& chunkManager = Bootstrap_->GetChunkManager();

    TCompactMediumMap<TNodeList> result;
    auto chunkReplication = GetChunkAggregatedReplication(chunk, replicas);
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

    auto temporarilyUnavailablePartCount = 0;
    NErasure::TPartIndexSet temporarilyUnavailableIndexes;
    auto statisticsReplicaMissingStatus = EChunkStatus::None;

    for (int index = 0; index < totalPartCount; ++index) {
        int replicaCount = result.ReplicaCount[index];
        int decommissionedReplicaCount = result.DecommissionedReplicaCount[index];
        int temporarilyUnavailableReplicaCount = result.TemporarilyUnavailableReplicaCount[index];
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

            if (temporarilyUnavailableReplicaCount > 0) {
                result.Status |= EChunkStatus::TemporarilyUnavailable;
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
            statisticsReplicaMissingStatus |= isDataPart ? EChunkStatus::DataMissing : EChunkStatus::ParityMissing;

            if (temporarilyUnavailableReplicaCount > 0) {
                ++temporarilyUnavailablePartCount;
                temporarilyUnavailableIndexes.set(index);
            } else {
                erasedIndexes.set(index);
                result.Status |= statisticsReplicaMissingStatus;
            }
        }
    }

    auto isTemporaryUnavailabilitySafe =
        temporarilyUnavailablePartCount + MinSafeAvailableReplicaCount <=
        codec->GetGuaranteedRepairablePartCount() + 1;
    if (erasedIndexes.any() || !isTemporaryUnavailabilitySafe)
    {
        result.Status |= statisticsReplicaMissingStatus;
        erasedIndexes |= temporarilyUnavailableIndexes;
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

TChunkReplicator::TChunkStatistics TChunkReplicator::ComputeRegularChunkStatistics(
    const TChunk* chunk,
    const TChunkLocationPtrWithReplicaInfoList& replicas)
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
    TMediumMap<int> temporarilyUnavailableReplicaCount;
    TMediumMap<TChunkLocationPtrWithReplicaIndexList> decommissionedReplicas;
    int totalReplicaCount = 0;
    int totalDecommissionedReplicaCount = 0;

    TMediumMap<TNodePtrWithReplicaAndMediumIndexList> missingReplicas;

    TMediumSet hasSealedReplica;
    bool hasSealedReplicas = false;
    bool totallySealed = chunk->IsSealed();

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto consistentPlacementNodes = GetChunkConsistentPlacementNodes(chunk, replicas);

    for (const auto& [mediumIndex, consistentNodes] : consistentPlacementNodes) {
        for (auto node : consistentNodes) {
            TNodePtrWithReplicaAndMediumIndex nodePtrWithIndexes(node, GenericChunkReplicaIndex, mediumIndex);
            auto it = std::find_if(
                replicas.begin(),
                replicas.end(),
                [&, mediumIndex = mediumIndex] (const auto& replica) {
                    return GetChunkLocationNode(replica) == node && replica.GetPtr()->GetEffectiveMediumIndex() == mediumIndex;
                });
            if (it == replicas.end()) {
                missingReplicas[mediumIndex].push_back(nodePtrWithIndexes);
            }
        }
    }

    for (auto replica : replicas) {
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
        } else if (IsReplicaOnPendingRestartNode(replica.GetPtr())) {
            ++temporarilyUnavailableReplicaCount[mediumIndex];
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

    auto replication = GetChunkAggregatedReplication(chunk, replicas);
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
        auto mediumTemporarilyUnavailableReplicaCount = temporarilyUnavailableReplicaCount[mediumIndex];

        // NB: some very counter-intuitive scenarios are possible here.
        // E.g. mediumReplicationFactor == 0, but mediumReplicaCount != 0.
        // This happens when chunk's requisition changes. One should be careful
        // with one's assumptions.
        if (!mediumReplicationPolicy &&
            mediumReplicaCount == 0 &&
            mediumDecommissionedReplicaCount == 0 &&
            mediumTemporarilyUnavailableReplicaCount == 0)
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
            mediumTemporarilyUnavailableReplicaCount,
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
    int temporarilyUnavailableReplicaCount,
    const TChunkLocationPtrWithReplicaIndexList& decommissionedReplicas,
    bool hasSealedReplica,
    bool totallySealed,
    TChunkLocationPtrWithReplicaInfo unsafelyPlacedReplica,
    TChunkLocationPtrWithReplicaIndex inconsistentlyPlacedReplica,
    const TNodePtrWithReplicaAndMediumIndexList& missingReplicas)
{
    auto replicationFactor = replicationPolicy.GetReplicationFactor();
    auto maxSafeTemporarilyUnavailableReplicaCount = static_cast<int>(replicationFactor * MaxSafeTemporarilyUnavailableReplicaFraction);
    auto minSafeUnavailableReplicas = std::max(
        replicationFactor - maxSafeTemporarilyUnavailableReplicaCount,
        std::min(replicationFactor, MinSafeAvailableReplicaCount));
    auto totalReplicaCount = replicaCount + decommissionedReplicaCount + temporarilyUnavailableReplicaCount;

    result.ReplicaCount[GenericChunkReplicaIndex] = replicaCount;
    result.TemporarilyUnavailableReplicaCount[GenericChunkReplicaIndex] = temporarilyUnavailableReplicaCount;
    result.DecommissionedReplicaCount[GenericChunkReplicaIndex] = decommissionedReplicaCount;
    result.MissingReplicas = missingReplicas;

    if (replicaCount + decommissionedReplicaCount == 0) {
        result.Status |= EChunkStatus::Lost;
    }

    if (chunk->IsSealed()) {
        if (chunk->IsJournal() && replicationFactor > 0 && !hasSealedReplica) {
            result.Status |= EChunkStatus::SealedMissing;
        }

        if ((replicaCount + temporarilyUnavailableReplicaCount < replicationFactor ||
             replicaCount < minSafeUnavailableReplicas) &&
             hasSealedReplica)
        {
            result.Status |= EChunkStatus::Underreplicated;
        }

        if (temporarilyUnavailableReplicaCount > 0) {
            result.Status |= EChunkStatus::TemporarilyUnavailable;
        }

        if (totallySealed && totalReplicaCount > replicationFactor) {
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
            } else if (replicaCount > replicationFactor) {
                result.BalancingRemovalIndexes.push_back(GenericChunkReplicaIndex);
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
    for (auto& queue : node->ChunkPullReplicationQueues()) {
        for (const auto& [chunkReplica, mediumSet] : queue) {
            auto* chunk = chunkManager->FindChunk(chunkReplica.Id);
            if (IsObjectAlive(chunk)) {
                ScheduleChunkRefresh(chunk);
            }
        }
    }

    auto unlockChunks = [&] (const auto& chunkIds) {
        for (auto chunkId : chunkIds) {
            EraseOrCrash(RemovalLockedChunkIds_, chunkId);
        }
    };

    unlockChunks(node->RemovalJobScheduledChunkIds());
    node->RemovalJobScheduledChunkIds().clear();

    unlockChunks(GetValues(node->AwaitingHeartbeatChunkIds()));
    node->AwaitingHeartbeatChunkIds().clear();
}

void TChunkReplicator::OnChunkDestroyed(TChunk* chunk)
{
    // NB: We have to handle chunk here even if it should not be processed by replicator
    // since it may be in some of the queues being put when replicator processed this chunk.
    GetChunkRefreshScanner(chunk)->OnChunkDestroyed(chunk);
    GetChunkRequisitionUpdateScanner(chunk)->OnChunkDestroyed(chunk);
    ResetChunkStatus(chunk);
    RemoveFromChunkRepairQueues(chunk);
}

void TChunkReplicator::RemoveFromChunkReplicationQueues(
    TNode* node,
    TChunkIdWithIndex chunkIdWithIndex)
{
    auto chunkId = chunkIdWithIndex.Id;
    const auto& pullReplicationNodeIds = node->PushReplicationTargetNodeIds();
    if (auto it = pullReplicationNodeIds.find(chunkId); it != pullReplicationNodeIds.end()) {
        for (auto [mediumIndex, nodeId] : it->second) {
            UnrefChunkBeingPulled(nodeId, chunkId, mediumIndex);
        }
    }
    node->RemoveFromChunkReplicationQueues(chunkIdWithIndex);
}

void TChunkReplicator::OnReplicaRemoved(
    TChunkLocation* location,
    TChunkPtrWithReplicaIndex replica,
    ERemoveReplicaReason reason)
{
    auto* chunk = replica.GetPtr();
    auto* node = location->GetNode();

    auto chunkIdWithIndex = TChunkIdWithIndex(chunk->GetId(), replica.GetReplicaIndex());
    // NB: It's OK to remove all replicas from replication queues here because
    // if some replica is removed we need to schedule chunk refresh anyway.
    RemoveFromChunkReplicationQueues(node, chunkIdWithIndex);
    if (reason != ERemoveReplicaReason::ChunkDestroyed) {
        location->RemoveFromChunkRemovalQueue(chunkIdWithIndex);
    }
    if (chunk->IsJournal()) {
        location->RemoveFromChunkSealQueue(chunkIdWithIndex);
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
    TNodeId targetNodeId,
    const TChunkLocationPtrWithReplicaInfoList& replicas)
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
    auto replicationFactor = GetChunkAggregatedReplicationFactor(chunk, targetMediumIndex);

    auto statistics = ComputeChunkStatistics(chunk, replicas);
    const auto& mediumStatistics = statistics.PerMediumStatistics[targetMediumIndex];
    int replicaCount = mediumStatistics.ReplicaCount[replicaIndex];
    int temporarilyUnavailableReplicaCount = mediumStatistics.TemporarilyUnavailableReplicaCount[replicaIndex];

    if (Any(statistics.Status & ECrossMediumChunkStatus::Lost)) {
        return true;
    }

    if (replicaCount > replicationFactor) {
        return true;
    }

    int replicasNeeded;
    if (Any(mediumStatistics.Status & EChunkStatus::Underreplicated)) {
        replicasNeeded = std::min(
            replicationFactor,
            std::max(
                replicationFactor - temporarilyUnavailableReplicaCount,
                MinSafeAvailableReplicaCount)) - replicaCount;
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
        replicas,
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
        if (RemovalLockedChunkIds_.contains(chunkIdWithIndexes.Id)) {
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

    if (IsObjectAlive(chunk)) {
        auto& jobScheduledChunkIds = context->GetNode()->RemovalJobScheduledChunkIds();
        EmplaceOrCrash(jobScheduledChunkIds, chunkIdWithIndexes.Id);
        EmplaceOrCrash(RemovalLockedChunkIds_, chunkIdWithIndexes.Id);
    }

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
    TChunkPtrWithReplicaAndMediumIndex chunkWithIndexes,
    const TChunkLocationPtrWithReplicaInfoList& replicas)
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

    auto statistics = ComputeChunkStatistics(chunk, replicas);
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
        replicas,
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
    // Keep the ability to disable scheduling completely.
    if (GetDynamicConfig()->MaxMisscheduledReplicationJobsPerHeartbeat == 0) {
        return;
    }

    auto* node = context->GetNode();
    auto nodeHolder = TEphemeralObjectPtr<TNode>(node);

    const auto& resourceUsage = context->GetNodeResourceUsage();
    const auto& resourceLimits = context->GetNodeResourceLimits();

    int misscheduledPullReplicationJobs = 0;
    // NB: To avoid starvation we account misscheduled jobs per priority.
    THashMap<int, int> misscheduledPushReplicationJobsPerPriority;

    const auto& chunkManager = Bootstrap_->GetChunkManager();

    auto maxReplicationJobCount = resourceLimits.replication_slots();

    // NB: Beware of chunks larger than the limit; we still need to be able to replicate them one by one.
    auto hasSparePushReplicationResources = [&] (int priority) {
        return
            misscheduledPushReplicationJobsPerPriority[priority] < GetDynamicConfig()->MaxMisscheduledReplicationJobsPerHeartbeat &&
            resourceUsage.replication_slots() < resourceLimits.replication_slots() &&
            (resourceUsage.replication_slots() == 0 || resourceUsage.replication_data_size() < resourceLimits.replication_data_size());
    };

    const auto& incumbentManager = Bootstrap_->GetIncumbentManager();
    auto incumbentCount = incumbentManager->GetIncumbentCount(EIncumbentType::ChunkReplicator);
    auto hasSparePullReplicationResources = [&] {
        auto maxPullReplicationJobs = GetDynamicConfig()->MaxRunningReplicationJobsPerTargetNode / std::max(incumbentCount, 1);
        return
            // TODO(gritukan): Use some better bounds.
            misscheduledPullReplicationJobs < GetDynamicConfig()->MaxMisscheduledReplicationJobsPerHeartbeat &&
            std::ssize(node->ChunksBeingPulled()) < maxPullReplicationJobs;
    };

    THashMap<TChunkId, std::vector<std::pair<int, int>>> pullChunkIdToStuff;
    std::vector<TEphemeralObjectPtr<TChunk>> pullChunks;
    auto& queues = node->ChunkPullReplicationQueues();
    for (int priority = 0; priority < std::ssize(queues); ++priority) {
        auto& queue = queues[priority];
        auto chunkCount = queue.size();
        for (int index = 0; index < static_cast<int>(chunkCount); ++index) {
            if (!hasSparePullReplicationResources() || std::ssize(pullChunks) >= maxReplicationJobCount) {
                break;
            }

            if (queue.empty()) {
                break;
            }

            auto chunkIt = queue.PickRandomChunk();
            auto chunkIdWithIndex = chunkIt->first;
            auto chunkId = chunkIdWithIndex.Id;
            auto* chunk = chunkManager->FindChunk(chunkId);

            if (!IsObjectAlive(chunk) || !chunk->IsRefreshActual()) {
                queue.Erase(chunkIt);
                ++misscheduledPullReplicationJobs;
                continue;
            }

            pullChunks.emplace_back(chunk);
            pullChunkIdToStuff[chunkId].emplace_back(priority, chunkIdWithIndex.ReplicaIndex);
        }
    }

    auto pullReplicas = chunkManager->GetChunkReplicas(pullChunks);

    // Move CRP-enabled chunks from pull to push queues.
    for (const auto& [chunkId, replicasOrError] : pullReplicas) {
        TForbidContextSwitchGuard guard;

        for (auto [priority, index] : GetOrCrash(pullChunkIdToStuff, chunkId)) {
            TChunkIdWithIndex chunkIdWithIndex(chunkId, index);

            auto& queue = node->ChunkPullReplicationQueues()[priority];
            auto it = queue.find(chunkIdWithIndex);
            if (it == queue.end()) {
                ++misscheduledPullReplicationJobs;
                continue;
            }

            auto* chunk = chunkManager->FindChunk(chunkId);

            if (!IsObjectAlive(chunk) || !chunk->IsRefreshActual()) {
                queue.Erase(it);
                ++misscheduledPullReplicationJobs;
                continue;
            }

            if (!replicasOrError.IsOK()) {
                queue.Erase(it);
                ++misscheduledPullReplicationJobs;
                ScheduleChunkRefresh(chunk);
                continue;
            }

            auto desiredReplica = it->first;
            auto& mediumIndexSet = it->second;

            for (const auto& replica : replicasOrError.Value()) {
                auto* pushNode = GetChunkLocationNode(replica);
                for (int mediumIndex = 0; mediumIndex < std::ssize(mediumIndexSet); ++mediumIndex) {
                    if (mediumIndexSet.test(mediumIndex)) {
                        if (pushNode->GetTargetReplicationNodeId(chunkId, mediumIndex) != InvalidNodeId) {
                            // Replication is already planned with another node as a destination.
                            continue;
                        }

                        if (desiredReplica.ReplicaIndex != replica.GetReplicaIndex()) {
                            continue;
                        }

                        TChunkIdWithIndex chunkIdWithIndex(chunkId, replica.GetReplicaIndex());
                        pushNode->AddToChunkPushReplicationQueue(chunkIdWithIndex, mediumIndex, priority);
                        pushNode->AddTargetReplicationNodeId(chunkId, mediumIndex, node);

                        node->RefChunkBeingPulled(chunkId, mediumIndex);
                    }
                }
            }

            queue.Erase(it);
        }
    }

    THashMap<TChunkId, std::vector<std::pair<int, int>>> chunkIdToStuff;
    std::vector<TEphemeralObjectPtr<TChunk>> chunksToReplicate;
    // Gather chunks for replica fetch.
    for (int priority = 0; priority < std::ssize(node->ChunkPushReplicationQueues()); ++priority) {
        auto& queue = node->ChunkPushReplicationQueues()[priority];
        // NB: We take chunks from every queue to avoid starvation.
        auto chunkCount = std::min<int>(maxReplicationJobCount, queue.size());
        for (int index = 0; index < chunkCount; ++index) {
            if (queue.empty()) {
                break;
            }

            auto it = queue.PickRandomChunk();
            auto chunkIdWithIndex = it->first;
            auto chunkId = chunkIdWithIndex.Id;
            auto* chunk = chunkManager->FindChunk(chunkId);
            if (!IsObjectAlive(chunk) || !chunk->IsRefreshActual()) {
                // NB: Call below removes chunk from #queue.
                RemoveFromChunkReplicationQueues(node, chunkIdWithIndex);
                ++misscheduledPushReplicationJobsPerPriority[priority];
                continue;
            }

            chunksToReplicate.emplace_back(chunk);
            chunkIdToStuff[chunkId].emplace_back(priority, chunkIdWithIndex.ReplicaIndex);
        }
    }

    // TODO(aleksandra-zh): maybe unite this with getting pull chunk replicas.
    auto chunkReplicas = chunkManager->GetChunkReplicas(chunksToReplicate);

    // Schedule replication jobs. Iterate chunks to preserve order.
    for (const auto& chunk : chunksToReplicate) {
        TForbidContextSwitchGuard guard;
        auto chunkId = chunk->GetId();
        const auto& replicasOrError = GetOrCrash(chunkReplicas, chunkId);
        for (auto [priority, index] : GetOrCrash(chunkIdToStuff, chunkId)) {
            if (!hasSparePushReplicationResources(priority)) {
                continue;
            }

            TChunkIdWithIndex chunkIdWithIndex(chunkId, index);

            auto& queue = node->ChunkPushReplicationQueues()[priority];
            auto it = queue.find(chunkIdWithIndex);
            if (it == queue.end()) {
                // NB: Call below removes chunk from #queue.
                RemoveFromChunkReplicationQueues(node, chunkIdWithIndex);
                ++misscheduledPushReplicationJobsPerPriority[priority];
                continue;
            }

            if (!IsObjectAlive(chunk) || !chunk->IsRefreshActual()) {
                // NB: Call below removes chunk from #queue.
                RemoveFromChunkReplicationQueues(node, chunkIdWithIndex);
                ++misscheduledPushReplicationJobsPerPriority[priority];
                continue;
            }

            if (!replicasOrError.IsOK()) {
                RemoveFromChunkReplicationQueues(node, chunkIdWithIndex);
                ++misscheduledPushReplicationJobsPerPriority[priority];
                ScheduleChunkRefresh(chunk.Get());
                continue;
            }

            auto& mediumIndexSet = it->second;
            const auto& replicas = replicasOrError.Value();
            for (int mediumIndex = 0; mediumIndex < std::ssize(mediumIndexSet); ++mediumIndex) {
                if (mediumIndexSet.test(mediumIndex)) {
                    auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                    if (!IsObjectAlive(medium)) {
                        YT_LOG_ALERT(
                            "Attempted to schedule replication job for non-existent medium, ignored "
                            "(ChunkId: %v, MediumIndex: %v)",
                            chunk->GetId(),
                            mediumIndex);
                        ++misscheduledPushReplicationJobsPerPriority[priority];
                        // Something bad happened, let's try to forget it.
                        mediumIndexSet.reset(mediumIndex);
                        ScheduleChunkRefresh(chunk.Get());
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
                        ++misscheduledPushReplicationJobsPerPriority[priority];
                        // Something bad happened, let's try to forget it.
                        mediumIndexSet.reset(mediumIndex);
                        ScheduleChunkRefresh(chunk.Get());
                        continue;
                    }

                    auto nodeId = node->GetTargetReplicationNodeId(chunkId, mediumIndex);
                    node->RemoveTargetReplicationNodeId(chunkId, mediumIndex);

                    if (TryScheduleReplicationJob(
                        context,
                        {chunk.Get(), chunkIdWithIndex.ReplicaIndex},
                        medium->AsDomestic(),
                        nodeId,
                        replicas))
                    {
                        mediumIndexSet.reset(mediumIndex);
                    } else {
                        ++misscheduledPushReplicationJobsPerPriority[priority];
                        if (nodeId != InvalidNodeId) {
                            mediumIndexSet.reset(mediumIndex);
                            // Move all CRP-enabled chunks with misscheduled jobs back to pull queue.
                            ScheduleChunkRefresh(chunk.Get());
                        }
                    }
                }
            }

            if (mediumIndexSet.none()) {
                queue.Erase(it);
            }
        }
    }

    MisscheduledJobs_[EJobType::ReplicateChunk] += misscheduledPullReplicationJobs;
    for (const auto& [priority, count] : misscheduledPushReplicationJobsPerPriority) {
        MisscheduledJobs_[EJobType::ReplicateChunk] += count;
    }
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

    if (GetActiveShardCount() > 0) {
        LocationShards_.clear();
        for (auto* location : node->ChunkLocations()) {
            for (auto shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
                if (IsShardActive(shardIndex) &&
                    !location->GetDestroyedReplicaSet(shardIndex).empty())
                {
                    LocationShards_.push_back({location, location->GetDestroyedReplicasIterator(shardIndex), shardIndex});
                }
            }
        }
        int activeLocationCount = std::ssize(LocationShards_);

        while (activeLocationCount > 0 && hasSpareRemovalResources()) {
            for (auto& [location, replicaIterator, shardId, active] : LocationShards_) {
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

                if (TryScheduleRemovalJob(context, replica, location->IsImaginary() ? nullptr : location->AsReal())) {
                    location->SetDestroyedReplicasIterator(replicaIterator, shardId);
                } else {
                    ++misscheduledRemovalJobs;
                }
            }
        }
    }

    {
        TCompactVector<
            std::pair<TChunkLocation*, TChunkLocation::TChunkQueue::iterator>,
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
    const auto& chunkManager = Bootstrap_->GetChunkManager();

    auto* node = context->GetNode();

    const auto& resourceUsage = context->GetNodeResourceUsage();
    const auto& resourceLimits = context->GetNodeResourceLimits();

    auto maxRepairJobs = resourceLimits.repair_slots();
    int misscheduledRepairJobs = 0;

    // NB: Beware of chunks larger than the limit; we still need to be able to repair them one by one.
    auto hasSpareRepairResources = [&] {
        return
            misscheduledRepairJobs < GetDynamicConfig()->MaxMisscheduledRepairJobsPerHeartbeat &&
            resourceUsage.repair_slots() < resourceLimits.repair_slots() &&
            (resourceUsage.repair_slots() == 0 || resourceUsage.repair_data_size() < resourceLimits.repair_data_size());
    };

    std::vector<TEphemeralObjectPtr<TChunk>> chunks;
    THashMap<TChunkId, std::vector<std::pair<int, EChunkRepairQueue>>> chunkPartsInfo;

    // Schedule repair jobs.
    // NB: the order of the enum items is crucial! Part-missing chunks must
    // be repaired before part-decommissioned chunks.
    for (auto queue : TEnumTraits<EChunkRepairQueue>::GetDomainValues()) {
        TMediumMap<std::pair<TChunkRepairQueue::iterator, TChunkRepairQueue::iterator>> iteratorPerRepairQueue;
        for (int mediumIndex = 0; mediumIndex < MaxMediumCount; ++mediumIndex) {
            auto& chunkRepairQueue = ChunkRepairQueue(mediumIndex, queue);
            if (!chunkRepairQueue.empty()) {
                iteratorPerRepairQueue[mediumIndex] = std::pair(chunkRepairQueue.begin(), chunkRepairQueue.end());
            }
        }

        while (std::ssize(chunks) < maxRepairJobs) {
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
            auto chunkIt = iteratorPerRepairQueue[mediumIndex].first++;
            auto* chunk = chunkIt->GetPtr();
            if (!IsObjectAlive(chunk)) {
                // Chunk should be removed from queues elsewhere.
                ++misscheduledRepairJobs;
                continue;
            }

            chunks.emplace_back(chunk);
            chunkPartsInfo[chunk->GetId()].emplace_back(mediumIndex, queue);
        }
    }

    auto replicas = chunkManager->GetChunkReplicas(chunks);

    // Schedule repair jobs.
    for (const auto& chunk : chunks) {
        if (!hasSpareRepairResources()) {
            break;
        }

        if (!IsObjectAlive(chunk)) {
            // Chunk should be removed from queues elsewhere.
            ++misscheduledRepairJobs;
            continue;
        }

        auto chunkId = chunk->GetId();
        const auto& replicasOrError = GetOrCrash(replicas, chunkId);
        for (auto [mediumIndex, queue] : GetOrCrash(chunkPartsInfo, chunkId)) {
            auto& chunkRepairQueue = ChunkRepairQueue(mediumIndex, queue);
            auto* chunk = chunkManager->FindChunk(chunkId);
            if (!IsObjectAlive(chunk)) {
                // Chunk should be removed from queues elsewhere.
                ++misscheduledRepairJobs;
                continue;
            }

            auto chunkIt = chunk->GetRepairQueueIterator(mediumIndex, queue);
            if (chunkIt == TChunkRepairQueueIterator()) {
                // If someone discarded iterator, they have probably removed chunk from queue as well, so do nothing.
                ++misscheduledRepairJobs;
                continue;
            }

            auto removeFromQueue = [&, mediumIndex = mediumIndex, queue = queue] {
                chunk->SetRepairQueueIterator(mediumIndex, queue, TChunkRepairQueueIterator());
                chunkRepairQueue.erase(chunkIt);
            };

            // NB: Repair queues are not cleared when shard processing is stopped,
            // so we have to handle chunks replicator should not process.
            TChunkPtrWithReplicaAndMediumIndex chunkWithIndexes(chunk, GenericChunkReplicaIndex, mediumIndex);
            if (!chunk->IsRefreshActual() || !replicasOrError.IsOK()) {
                removeFromQueue();
                ++misscheduledRepairJobs;
            } else if (TryScheduleRepairJob(context, queue, chunkWithIndexes, replicasOrError.Value())) {
                removeFromQueue();
            } else {
                ++misscheduledRepairJobs;
            }
        }
    }

    MisscheduledJobs_[EJobType::RepairChunk] += misscheduledRepairJobs;
}

void TChunkReplicator::RefreshChunk(
    const TEphemeralObjectPtr<TChunk>& ephemeralChunk,
    const TChunkLocationPtrWithReplicaInfoList& chunkReplicas)
{
    if (!ephemeralChunk->IsConfirmed()) {
        return;
    }

    if (ephemeralChunk->IsForeign()) {
        return;
    }

    if (!IsObjectAlive(ephemeralChunk)) {
        return;
    }

    auto* chunk = ephemeralChunk.Get();
    auto chunkId = chunk->GetId();
    const auto& chunkManager = Bootstrap_->GetChunkManager();

    chunk->OnRefresh();

    ResetChunkStatus(chunk);
    RemoveFromChunkRepairQueues(chunk);

    RemoveChunkReplicasFromReplicationQueues(chunkId, chunkReplicas);

    auto replication = GetChunkAggregatedReplication(chunk, chunkReplicas);

    auto allMediaStatistics = ComputeChunkStatistics(chunk, chunkReplicas);

    auto durabilityRequired = IsDurabilityRequired(chunk, chunkReplicas);

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

        if (Any(statistics.Status & EChunkStatus::TemporarilyUnavailable)) {
            TemporarilyUnavailableChunks_.insert(chunk);
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
                        chunkId,
                        locationWithIndexes.GetReplicaIndex(),
                        location->GetEffectiveMediumIndex());
                    location->AddToChunkRemovalQueue(chunkIdWithIndexes);
                }

                for (int replicaIndex : statistics.BalancingRemovalIndexes) {
                    TChunkPtrWithReplicaAndMediumIndex chunkWithIndexes(chunk, replicaIndex, mediumIndex);
                    auto* targetLocation = ChunkPlacement_->GetRemovalTarget(chunkWithIndexes, chunkReplicas);
                    if (!targetLocation) {
                        continue;
                    }

                    TChunkIdWithIndexes chunkIdWithIndexes(chunkId, replicaIndex, mediumIndex);
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
                                    chunkId,
                                    node->GetId());
                                continue;
                            }

                            node->AddToChunkPullReplicationQueue(
                                {chunkId, replica.GetReplicaIndex()},
                                mediumIndex,
                                priority);
                        }
                    } else {
                        for (auto replica : chunkReplicas) {
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
                            if (!node->ReportedDataNodeHeartbeat() || node->IsPendingRestart()) {
                                continue;
                            }

                            TChunkIdWithIndex chunkIdWithIndex(chunkId, replica.GetReplicaIndex());
                            node->AddToChunkPushReplicationQueue(chunkIdWithIndex, mediumIndex, priority);
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
        for (auto replica : chunkReplicas) {
            if (replica.GetReplicaState() != EChunkReplicaState::Unsealed) {
                continue;
            }

            auto* location = replica.GetPtr();
            if (!location->GetNode()->ReportedDataNodeHeartbeat()) {
                continue;
            }

            location->AddToChunkSealQueue({chunk->GetId(), replica.GetReplicaIndex()});
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
        ChunkIdsPendingEndorsementRegistration_.push_back(chunkId);
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
    TemporarilyUnavailableChunks_.erase(chunk);
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

void TChunkReplicator::RemoveChunkReplicasFromReplicationQueues(
    TChunkId chunkId,
    const TChunkLocationPtrWithReplicaInfoList& replicas)
{
    for (auto replica : replicas) {
        auto* location = replica.GetPtr();
        auto* node = location->GetNode();
        int mediumIndex = location->GetEffectiveMediumIndex();

        RemoveFromChunkReplicationQueues(node, {chunkId, replica.GetReplicaIndex()});

        TChunkIdWithIndexes chunkIdWithIndexes(chunkId, replica.GetReplicaIndex(), mediumIndex);
        location->RemoveFromChunkRemovalQueue(chunkIdWithIndexes);
    }
}

bool TChunkReplicator::IsReplicaDecommissioned(TChunkLocation* replica)
{
    return replica->GetNode()->IsDecommissioned();
}

bool TChunkReplicator::IsReplicaOnPendingRestartNode(TChunkLocation* replica)
{
    return replica->GetNode()->IsPendingRestart();
}

TChunkReplication TChunkReplicator::GetChunkAggregatedReplication(
    const TChunk* chunk,
    const TChunkLocationPtrWithReplicaInfoList& replicas) const
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
    for (auto replica : replicas) {
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

    GetChunkRefreshScanner(chunk)->EnqueueChunk({chunk, /*errorCount*/ 0});
}

void TChunkReplicator::ScheduleNodeRefresh(TNode* node)
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto hasSequoiaMedia = false;

    for (auto* location : node->ChunkLocations()) {
        const auto* medium = chunkManager->FindMediumByIndex(location->GetEffectiveMediumIndex());
        if (!medium) {
            continue;
        }

        if (medium->IsDomestic()) {
            const auto* domesticMedium = medium->AsDomestic();
            hasSequoiaMedia |= domesticMedium->GetEnableSequoiaReplicas();
        }

        for (auto replica : location->Replicas()) {
            ScheduleChunkRefresh(replica.GetPtr());
        }
    }

    // It seems okay to loose ScheduleNodeRefreshSequoia on epoch end as global refresh
    // will take care of all chunks in that case.
    // Global refresh also makes scheduling this callback during recovery redundant.
    if (hasSequoiaMedia && !Bootstrap_->GetHydraFacade()->GetHydraManager()->IsRecovery()) {
        auto invoker = Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkRefresher);
        invoker->Invoke(BIND(&TChunkReplicator::ScheduleNodeRefreshSequoia, MakeStrong(this), node->GetId()));
    }
}

void TChunkReplicator::ScheduleNodeRefreshSequoia(TNodeId nodeId)
{
    YT_VERIFY(!HasMutationContext());

    const auto& chunkManager = Bootstrap_->GetChunkManager();

    auto sequoiaReplicasFuture = chunkManager->GetSequoiaNodeReplicas(nodeId);
    auto sequoiaReplicasOrError = WaitFor(sequoiaReplicasFuture);
    if (!sequoiaReplicasOrError.IsOK()) {
        YT_LOG_ERROR(sequoiaReplicasOrError, "Error getting Sequoia node replicas");
        return;
    }

    const auto& sequoiaReplicas = sequoiaReplicasOrError.ValueOrThrow();
    for (const auto& replica : sequoiaReplicas) {
        auto* chunk = chunkManager->FindChunk(replica.Key.ChunkId);
        if (IsObjectAlive(chunk)) {
            ScheduleChunkRefresh(chunk);
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
    auto config = GetDynamicConfig();
    if (!config->EnableChunkRefresh) {
        YT_LOG_DEBUG("Chunk refresh disabled");
        return;
    }

    YT_LOG_DEBUG("Chunk refresh iteration started");

    auto deadline = GetCpuInstant() - DurationToCpuDuration(config->ChunkRefreshDelay);

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto doRefreshChunks = [&] (
        const std::unique_ptr<TChunkRefreshScanner>& scanner,
        int* const totalCount,
        int* const aliveCount,
        int* const replicasErrorCount,
        int maxChunksPerRefresh)
    {
        std::vector<TEphemeralObjectPtr<TChunk>> chunksToRefresh;
        THashMap<TChunkId, int> chunkIdToErrorCount;
        while (*totalCount < maxChunksPerRefresh && scanner->HasUnscannedChunk(deadline)) {
            ++(*totalCount);
            auto [chunk, errorCount] = scanner->DequeueChunk();
            if (!IsObjectAlive(chunk)) {
                continue;
            }

            chunksToRefresh.emplace_back(chunk);
            chunkIdToErrorCount[chunk->GetId()] = errorCount;
        }

        auto replicas = chunkManager->GetChunkReplicas(chunksToRefresh);
        for (const auto& chunk : chunksToRefresh) {
            if (!IsObjectAlive(chunk)) {
                continue;
            }

            const auto& replicasOrError = GetOrCrash(replicas, chunk->GetId());
            if (!replicasOrError.IsOK()) {
                auto refreshErrorCount = GetOrCrash(chunkIdToErrorCount, chunk->GetId());
                if (refreshErrorCount >= config->MaxUnsuccessfullRefreshAttempts) {
                    YT_LOG_ALERT("Too many unsuccessful refresh attempts for chunk (ChunkId: %v, RefreshErrorCount: %v, LastError: %v)",
                        chunk->GetId(),
                        refreshErrorCount,
                        replicasOrError);
                }
                scanner->EnqueueChunk({chunk.Get(), refreshErrorCount});
                ++(*replicasErrorCount);
                continue;
            }

            RefreshChunk(chunk, replicasOrError.Value());
            ++(*aliveCount);
        }
    };

    int totalBlobCount = 0;
    int totalJournalCount = 0;
    int aliveBlobCount = 0;
    int aliveJournalCount = 0;
    int replicasErrorCount = 0;
    int journalReplicasErrorCount = 0;

    ChunkIdsPendingEndorsementRegistration_.clear();

    YT_PROFILE_TIMING("/chunk_server/refresh_time") {
        doRefreshChunks(
            BlobRefreshScanner_,
            &totalBlobCount,
            &aliveBlobCount,
            &replicasErrorCount,
            config->MaxBlobChunksPerRefresh);
        doRefreshChunks(
            JournalRefreshScanner_,
            &totalJournalCount,
            &aliveJournalCount,
            &journalReplicasErrorCount,
            config->MaxJournalChunksPerRefresh);
    }

    FlushEndorsementQueue();

    YT_LOG_DEBUG("Chunk refresh iteration completed (TotalBlobCount: %v, AliveBlobCount: %v, ReplicasErrorCount: %v, TotalJournalCount: %v, AliveJournalCount: %v, JournalReplicasErrorCount: %v)",
        totalBlobCount,
        aliveBlobCount,
        replicasErrorCount,
        totalJournalCount,
        aliveJournalCount,
        journalReplicasErrorCount);

    // Journal replicas are always nonsequoia, so it is really concerning if this is nonzero.
    if (journalReplicasErrorCount > 0) {
        YT_LOG_ALERT("Chunk refresh iteration completed with nonzero journal replica errors (JournalReplicasErrorCount: %v)",
            journalReplicasErrorCount);
    }
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

bool TChunkReplicator::IsDurabilityRequired(
    TChunk* chunk,
    const TChunkLocationPtrWithReplicaInfoList& replicas) const
{
    if (chunk->GetHistoricallyNonVital()) {
        return false;
    }

    if (chunk->IsErasure()) {
        return true;
    }

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto replication = GetChunkAggregatedReplication(chunk, replicas);
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
    buffer->AddGauge("/chunk_ids_awaiting_requisition_update_scheduling", ChunkIdsAwaitingRequisitionUpdateScheduling_.size());

    buffer->AddGauge("/lost_chunk_count", LostChunks_.size());
    buffer->AddGauge("/lost_vital_chunk_count", LostVitalChunks_.size());
    buffer->AddGauge("/overreplicated_chunk_count", OverreplicatedChunks_.size());
    buffer->AddGauge("/unexpected_overreplicated_chunk_count", UnexpectedOverreplicatedChunks_.size());
    buffer->AddGauge("/underreplicated_chunk_count", UnderreplicatedChunks_.size());
    buffer->AddGauge("/data_missing_chunk_count", DataMissingChunks_.size());
    buffer->AddGauge("/parity_missing_chunk_count", ParityMissingChunks_.size());
    buffer->AddGauge("/temporarily_unavailable_chunk_count", TemporarilyUnavailableChunks_.size());
    buffer->AddGauge("/precarious_chunk_count", PrecariousChunks_.size());
    buffer->AddGauge("/precarious_vital_chunk_count", PrecariousVitalChunks_.size());
    buffer->AddGauge("/quorum_missing_chunk_count", QuorumMissingChunks_.size());
    buffer->AddGauge("/unsafely_placed_chunk_count", UnsafelyPlacedChunks_.size());
    buffer->AddGauge("/inconsistently_placed_chunk_count", InconsistentlyPlacedChunks_.size());
    buffer->AddGauge("/removal_locked_chunk_ids", RemovalLockedChunkIds_.size());

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
            if (!IsObjectAlive(node)) {
                continue;
            }

            TWithTagGuard tagGuard(crpBuffer, "node_address", node->GetDefaultAddress());

            i64 pullReplicationQueueSize = 0;
            for (const auto& queue : node->ChunkPullReplicationQueues()) {
                pullReplicationQueueSize += queue.size();
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

void TChunkReplicator::MaybeUpdateChunkRemovalLock(const TJobPtr& job)
{
    if (job->GetType() != EJobType::RemoveChunk) {
        return;
    }

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    auto* node = nodeTracker->GetNodeByAddress(job->NodeAddress());
    auto& jobScheduledChunkIds = node->RemovalJobScheduledChunkIds();
    auto& awaitingChunkIds = node->AwaitingHeartbeatChunkIds();

    const auto& chunkIdWithIndexes = job->GetChunkIdWithIndexes();
    auto chunkId = chunkIdWithIndexes.Id;
    auto it = jobScheduledChunkIds.find(chunkId);
    if (it != jobScheduledChunkIds.end()) {
        jobScheduledChunkIds.erase(it);

        auto sequenceNumber = job->GetSequenceNumber();
        awaitingChunkIds.emplace(sequenceNumber, chunkId);

        YT_LOG_DEBUG("Chunk removal job lock sequence number updated"
            " (ChunkId: %v, JobId: %v, SequenceNumber: %v)",
            chunkId,
            job->GetJobId(),
            sequenceNumber);
    }
}

void TChunkReplicator::OnJobCompleted(const TJobPtr& job)
{
    RemoveChunkFromPullReplicationSet(job);
    MaybeUpdateChunkRemovalLock(job);
}

void TChunkReplicator::OnJobAborted(const TJobPtr& job)
{
    RemoveChunkFromPullReplicationSet(job);
    MaybeUpdateChunkRemovalLock(job);
    TryRescheduleChunkRemoval(job);
}

void TChunkReplicator::OnJobFailed(const TJobPtr& job)
{
    RemoveChunkFromPullReplicationSet(job);
    MaybeUpdateChunkRemovalLock(job);
    TryRescheduleChunkRemoval(job);
}

void TChunkReplicator::ScheduleRequisitionUpdate(TChunkList* chunkList)
{
    // NB: only peer with specific shard index is responsible for chunk lists requisition traversal.
    if (!IsShardActive(ChunkListRequisitionUpdaterShardIndex)) {
        return;
    }

    if (!IsObjectAlive(chunkList)) {
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
            if (!IsObjectAlive(Root_)) {
                return;
            }
            auto callbacks = CreateAsyncChunkTraverserContext(
                Bootstrap_,
                NCellMaster::EAutomatonThreadQueue::ChunkRequisitionUpdateTraverser);
            TraverseChunkTree(std::move(callbacks), this, Root_.Get());
        }

    private:
        TBootstrap* const Bootstrap_;
        const TChunkReplicatorPtr Owner_;
        TEphemeralObjectPtr<TChunkList> const Root_;

        bool OnChunk(
            TChunk* chunk,
            TChunkList* /*parent*/,
            std::optional<i64> /*rowIndex*/,
            std::optional<int> /*tabletIndex*/,
            const NChunkClient::TReadLimit& /*startLimit*/,
            const NChunkClient::TReadLimit& /*endLimit*/,
            const TChunkViewModifier* /*modifier*/) override
        {
            Owner_->ChunkIdsAwaitingRequisitionUpdateScheduling_.push_back(chunk->GetId());
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
    if (!IsObjectAlive(chunk)) {
        return;
    }

    if (!ShouldProcessChunk(chunk)) {
        return;
    }

    GetChunkRequisitionUpdateScanner(chunk)->EnqueueChunk(chunk);
}

void TChunkReplicator::OnScheduleChunkRequisitionUpdatesFlush()
{
    if (!IsShardActive(ChunkListRequisitionUpdaterShardIndex)) {
        return;
    }

    if (!Bootstrap_->GetHydraFacade()->GetHydraManager()->IsActive()) {
        return;
    }

    auto limit = std::min<int>(
        GetDynamicConfig()->MaxChunksPerRequisitionUpdateScheduling,
        std::ssize(ChunkIdsAwaitingRequisitionUpdateScheduling_));

    TReqScheduleChunkRequisitionUpdates request;
    request.mutable_chunk_ids()->Reserve(limit);
    for (auto i = 0; i < limit; ++i) {
        ToProto(request.add_chunk_ids(), ChunkIdsAwaitingRequisitionUpdateScheduling_[i]);
    }

    if (request.chunk_ids_size() > 0) {
        YT_LOG_DEBUG("Flushing chunks scheduled for requisition update (Count: %v)",
            request.chunk_ids_size());

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto mutation = chunkManager->CreateScheduleChunkRequisitionUpdatesMutation(request);
        mutation->SetAllowLeaderForwarding(true);
        auto rspOrError = WaitFor(mutation->CommitAndLog(Logger));
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError,
                "Failed to schedule chunk requisition update flush");
            return;
        }
        ChunkIdsAwaitingRequisitionUpdateScheduling_.erase(
            ChunkIdsAwaitingRequisitionUpdateScheduling_.begin(),
            ChunkIdsAwaitingRequisitionUpdateScheduling_.begin() + request.chunk_ids_size());
    }
}

void TChunkReplicator::ScheduleGlobalRequisitionUpdate()
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
        if (IsShardActive(shardIndex)) {
            BlobRequisitionUpdateScanner_->ScheduleGlobalScan(chunkManager->GetGlobalBlobChunkScanDescriptor(shardIndex));
            JournalRequisitionUpdateScanner_->ScheduleGlobalScan(chunkManager->GetGlobalJournalChunkScanDescriptor(shardIndex));
        }
    }
}

void TChunkReplicator::OnRequisitionUpdate()
{
    if (!GetDynamicConfig()->EnableChunkRequisitionUpdate) {
        YT_LOG_DEBUG("Chunk requisition update disabled");
        return;
    }

    TReqUpdateChunkRequisition request;
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    request.set_cell_tag(ToProto<int>(multicellManager->GetCellTag()));

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
            if (!IsObjectAlive(chunk)) {
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
        auto mutation = chunkManager->CreateUpdateChunkRequisitionMutation(request);
        mutation->SetAllowLeaderForwarding(true);
        auto rspOrError = WaitFor(mutation->CommitAndLog(Logger));
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError,
                "Failed to update chunk requisition; Scheduling global scan");
            ScheduleGlobalRequisitionUpdate();
        }
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

    // BFS queue.
    TCompactQueue<TChunkList*, 64> queue;

    auto enqueue = [&] (TChunkList* chunkList) {
        if (chunkList->GetVisitMark() != mark) {
            chunkList->SetVisitMark(mark);
            queue.Push(chunkList);
        }
    };

    auto enqueueAdjustedParent = [&] (TChunkList* parent) {
        if (auto* adjustedParent = FollowParentLinks(parent)) {
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
    while (!queue.Empty()) {
        auto* chunkList = queue.Pop();
        // Examine owners, if any.
        for (const auto* owningNode : chunkList->TrunkOwningNodes()) {
            if (auto* account = owningNode->Account().Get()) {
                if (owningNode->GetHunkChunkList() == chunkList) {
                    requisition.AggregateWith(owningNode->HunkReplication(), account, true);
                } else {
                    requisition.AggregateWith(owningNode->Replication(), account, true);
                }
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

void TChunkReplicator::ConfirmChunkListRequisitionTraverseFinished(
    const TEphemeralObjectPtr<TChunkList>& chunkList)
{
    auto chunkListId = chunkList->GetId();
    YT_LOG_DEBUG("Chunk list requisition traverse finished (ChunkListId: %v)",
        chunkListId);
    ChunkListIdsWithFinishedRequisitionTraverse_.push_back(chunkListId);
}

void TChunkReplicator::OnFinishedRequisitionTraverseFlush()
{
    if (!IsShardActive(ChunkListRequisitionUpdaterShardIndex)) {
        return;
    }

    if (!Bootstrap_->GetHydraFacade()->GetHydraManager()->IsActive()) {
        return;
    }

    if (ChunkListIdsWithFinishedRequisitionTraverse_.empty()) {
        return;
    }

    YT_LOG_DEBUG("Flushing finished chunk lists requisition traverse confirmations (Count: %v)",
        ChunkListIdsWithFinishedRequisitionTraverse_.size());

    TReqConfirmChunkListsRequisitionTraverseFinished request;
    ToProto(request.mutable_chunk_list_ids(), ChunkListIdsWithFinishedRequisitionTraverse_);

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto mutation = chunkManager->CreateConfirmChunkListsRequisitionTraverseFinishedMutation(request);
    mutation->SetAllowLeaderForwarding(true);
    auto rspOrError = WaitFor(mutation->CommitAndLog(Logger));
    if (!rspOrError.IsOK()) {
        YT_LOG_WARNING(rspOrError,
            "Failed to flush finished requisition traverse");
        return;
    }
    ChunkListIdsWithFinishedRequisitionTraverse_.erase(
        ChunkListIdsWithFinishedRequisitionTraverse_.begin(),
        ChunkListIdsWithFinishedRequisitionTraverse_.begin() + request.chunk_list_ids_size());
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
    YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger)
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TMutationResponse>& error) {
            if (!error.IsOK()) {
                YT_LOG_WARNING(error,
                    "Failed to commit chunk endorsment registration mutation; "
                    "scheduling global refresh");
                ScheduleGlobalChunkRefresh();
            }
        }).AsyncVia(invoker)));
}

const std::unique_ptr<TChunkReplicator::TChunkRefreshScanner>& TChunkReplicator::GetChunkRefreshScanner(TChunk* chunk) const
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
    if (ScheduleChunkRequisitionUpdatesExecutor_) {
        ScheduleChunkRequisitionUpdatesExecutor_->SetPeriod(GetDynamicConfig()->ScheduledChunkRequisitionUpdatesFlushPeriod);
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

void TChunkReplicator::StartRefreshes(int shardIndex)
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    BlobRefreshScanner_->Start(chunkManager->GetGlobalBlobChunkScanDescriptor(shardIndex));
    JournalRefreshScanner_->Start(chunkManager->GetGlobalJournalChunkScanDescriptor(shardIndex));

    YT_LOG_INFO("Chunk refreshes started (ShardIndex: %v)",
        shardIndex);
}

void TChunkReplicator::StopRefreshes(int shardIndex)
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
    clearChunkSetShard(TemporarilyUnavailableChunks_);
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

    YT_LOG_INFO("Chunk refreshes stopped (ShardIndex: %v)",
        shardIndex);
}

void TChunkReplicator::StartRequisitionUpdates(int shardIndex)
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    BlobRequisitionUpdateScanner_->Start(chunkManager->GetGlobalBlobChunkScanDescriptor(shardIndex));
    JournalRequisitionUpdateScanner_->Start(chunkManager->GetGlobalJournalChunkScanDescriptor(shardIndex));

    //! We intentionally apply chunk lists incumbency logic here, mixing it with chunks,
    //! as placing it elsewhere would require major code changes with little payoff.
    if (shardIndex == ChunkListRequisitionUpdaterShardIndex) {
        chunkManager->RescheduleChunkListRequisitionTraversals();
    }

    YT_LOG_INFO("Chunk requisition updates started (ShardIndex: %v)",
        shardIndex);
}

void TChunkReplicator::StopRequisitionUpdates(int shardIndex)
{
    BlobRequisitionUpdateScanner_->Stop(shardIndex);
    JournalRequisitionUpdateScanner_->Stop(shardIndex);

    YT_LOG_INFO("Chunk requisition updates stopped (ShardIndex: %v)",
        shardIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
