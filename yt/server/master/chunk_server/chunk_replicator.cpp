#include "chunk_replicator.h"
#include "private.h"
#include "chunk.h"
#include "chunk_list.h"
#include "chunk_owner_base.h"
#include "chunk_placement.h"
#include "chunk_tree_traverser.h"
#include "chunk_view.h"
#include "job.h"
#include "chunk_scanner.h"
#include "chunk_replica.h"
#include "medium.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/world_initializer.h>
#include <yt/server/master/cell_master/multicell_manager.h>
#include <yt/server/master/cell_master/proto/multicell_manager.pb.h>

#include <yt/server/master/chunk_server/chunk_manager.h>

#include <yt/server/master/cypress_server/node.h>
#include <yt/server/master/cypress_server/cypress_manager.h>

#include <yt/server/master/node_tracker_server/data_center.h>
#include <yt/server/master/node_tracker_server/node.h>
#include <yt/server/master/node_tracker_server/node_directory_builder.h>
#include <yt/server/master/node_tracker_server/node_tracker.h>
#include <yt/server/master/node_tracker_server/rack.h>

#include <yt/server/master/object_server/object.h>

#include <yt/server/master/security_server/account.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/ytlib/node_tracker_client/helpers.h>
#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/client/object_client/helpers.h>

#include <yt/library/erasure/codec.h>

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/serialize.h>
#include <yt/core/misc/small_vector.h>
#include <yt/core/misc/string.h>

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/timing.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/ytree/ypath_proxy.h>

#include <array>
#include <yt/core/profiling/timing.h>

namespace NYT::NChunkServer {

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
using namespace NTransactionClient;

using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;
static const auto& Profiler = ChunkServerProfiler;

static NProfiling::TAggregateGauge RefreshTimeCounter("/refresh_time");
static NProfiling::TAggregateGauge RequisitionUpdateTimeCounter("/requisition_update_time");

////////////////////////////////////////////////////////////////////////////////

TChunkReplicator::TPerMediumChunkStatistics::TPerMediumChunkStatistics()
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
    , RefreshExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkMaintenance),
        BIND(&TChunkReplicator::OnRefresh, MakeWeak(this))))
    , RefreshScanner_(std::make_unique<TChunkScanner>(
        Bootstrap_->GetObjectManager(),
        EChunkScanKind::Refresh))
    , RequisitionUpdateExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkMaintenance),
        BIND(&TChunkReplicator::OnRequisitionUpdate, MakeWeak(this))))
    , RequisitionUpdateScanner_(std::make_unique<TChunkScanner>(
        Bootstrap_->GetObjectManager(),
        EChunkScanKind::RequisitionUpdate))
    , FinishedRequisitionTraverseFlushExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkMaintenance),
        BIND(&TChunkReplicator::OnFinishedRequisitionTraverseFlush, MakeWeak(this))))
    , MissingPartChunkRepairQueueBalancer_(
        Config_->RepairQueueBalancerWeightDecayFactor,
        Config_->RepairQueueBalancerWeightDecayInterval)
    , DecommissionedPartChunkRepairQueueBalancer_(
        Config_->RepairQueueBalancerWeightDecayFactor,
        Config_->RepairQueueBalancerWeightDecayInterval)
    , EnabledCheckExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Periodic),
        BIND(&TChunkReplicator::OnCheckEnabled, MakeWeak(this)),
        Config_->ReplicatorEnabledCheckPeriod))
    , JobThrottler_(CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>(),
        ChunkServerLogger,
        ChunkServerProfiler.AppendPath("/job_throttler")))
{
    YT_VERIFY(Config_);
    YT_VERIFY(Bootstrap_);
    YT_VERIFY(ChunkPlacement_);

    for (int i = 0; i < MaxMediumCount; ++i) {
        // We "balance" medium indexes, not the repair queues themselves.
        MissingPartChunkRepairQueueBalancer_.AddContender(i);
        DecommissionedPartChunkRepairQueueBalancer_.AddContender(i);
    }

    for (auto [_, node] : Bootstrap_->GetNodeTracker()->Nodes()) {
        if (node->GetLocalState() != ENodeState::Online) {
            continue;
        }
        for (const auto& replica : node->DestroyedReplicas()) {
            node->AddToChunkRemovalQueue(replica);
        }
    }

    InitInterDCEdges();
}

TChunkReplicator::~TChunkReplicator()
{ }

void TChunkReplicator::Start(TChunk* frontChunk, int chunkCount)
{
    RefreshScanner_->Start(frontChunk, chunkCount);
    RequisitionUpdateScanner_->Start(frontChunk, chunkCount);
    RefreshExecutor_->Start();
    RequisitionUpdateExecutor_->Start();
    FinishedRequisitionTraverseFlushExecutor_->Start();
    EnabledCheckExecutor_->Start();

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(DynamicConfigChangedCallback_);
    OnDynamicConfigChanged();
}

void TChunkReplicator::Stop()
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->UnsubscribeConfigChanged(DynamicConfigChangedCallback_);

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    for (const auto& nodePair : nodeTracker->Nodes()) {
        const auto* node = nodePair.second;
        for (const auto& jobPair : node->IdToJob()) {
            const auto& job = jobPair.second;
            auto* chunk = chunkManager->FindChunk(job->GetChunkIdWithIndexes().Id);
            if (chunk) {
                chunk->SetJob(nullptr);
            }
        }
    }

    for (const auto& queue : MissingPartChunkRepairQueues_) {
        for (auto chunkWithIndexes : queue) {
            chunkWithIndexes.GetPtr()->SetRepairQueueIterator(
                chunkWithIndexes.GetMediumIndex(),
                EChunkRepairQueue::Missing,
                TChunkRepairQueueIterator());
        }
    }
    MissingPartChunkRepairQueueBalancer_.ResetWeights();

    for (const auto& queue : DecommissionedPartChunkRepairQueues_) {
        for (auto chunkWithIndexes : queue) {
            chunkWithIndexes.GetPtr()->SetRepairQueueIterator(
                chunkWithIndexes.GetMediumIndex(),
                EChunkRepairQueue::Decommissioned,
                TChunkRepairQueueIterator());
        }
    }
    DecommissionedPartChunkRepairQueueBalancer_.ResetWeights();
}

void TChunkReplicator::TouchChunk(TChunk* chunk)
{
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
            YT_ABORT();
    }
}

TChunkReplicator::TChunkStatistics TChunkReplicator::ComputeRegularChunkStatistics(TChunk* chunk)
{
    TChunkStatistics result;

    TMediumMap<bool> hasUnsafelyPlacedReplicas{};
    TMediumMap<std::array<ui8, RackIndexBound>> perRackReplicaCounters{};

    TMediumIntMap replicaCount{};
    TMediumIntMap decommissionedReplicaCount{};
    TMediumMap<TNodePtrWithIndexesList> decommissionedReplicas;

    for (auto replica : chunk->StoredReplicas()) {
        auto mediumIndex = replica.GetMediumIndex();
        if (IsReplicaDecommissioned(replica)) {
            ++decommissionedReplicaCount[mediumIndex];
            decommissionedReplicas[mediumIndex].push_back(replica);
        } else {
            ++replicaCount[mediumIndex];
        }

        const auto* rack = replica.GetPtr()->GetRack();
        if (rack) {
            int rackIndex = rack->GetIndex();
            int maxReplicasPerRack = ChunkPlacement_->GetMaxReplicasPerRack(mediumIndex, chunk, std::nullopt);
            if (++perRackReplicaCounters[mediumIndex][rackIndex] > maxReplicasPerRack) {
                hasUnsafelyPlacedReplicas[mediumIndex] = true;
            }
        }
    }

    const auto replication = GetChunkAggregatedReplication(chunk);

    bool precarious = true;
    bool allMediaTransient = true;
    SmallVector<int, MaxMediumCount> mediaOnWhichLost;
    SmallVector<int, MaxMediumCount> mediaOnWhichPresent;
    int mediaOnWhichUnderreplicatedCount = 0;
    for (auto& entry : replication) {
        auto mediumIndex = entry.GetMediumIndex();
        auto* medium = Bootstrap_->GetChunkManager()->FindMediumByIndex(mediumIndex);
        YT_VERIFY(IsObjectAlive(medium));

        if (medium->GetCache()) {
            continue;
        }

        auto& mediumStatistics = result.PerMediumStatistics[mediumIndex];
        auto mediumTransient = medium->GetTransient();

        auto mediumReplicationFactor = entry.Policy().GetReplicationFactor();
        auto mediumReplicaCount = replicaCount[mediumIndex];
        auto mediumDecommissionedReplicaCount = decommissionedReplicaCount[mediumIndex];

        // NB: some very counter-intuitive scenarios are possible here.
        // E.g. mediumReplicationFactor == 0, but mediumReplicaCount != 0.
        // This happens when chunk's requisition changes. One should be careful
        // with one's assumptions.

        if (mediumReplicationFactor == 0 &&
            mediumReplicaCount == 0 &&
            mediumDecommissionedReplicaCount == 0)
        {
            // This medium is irrelevant to this chunk.
            continue;
        }

        ComputeRegularChunkStatisticsForMedium(
            mediumStatistics,
            mediumReplicationFactor,
            mediumReplicaCount,
            mediumDecommissionedReplicaCount,
            decommissionedReplicas[mediumIndex],
            hasUnsafelyPlacedReplicas[mediumIndex]);

        allMediaTransient = allMediaTransient && mediumTransient;

        if (Any(mediumStatistics.Status & EChunkStatus::Underreplicated)) {
            ++mediaOnWhichUnderreplicatedCount;
        }

        if (Any(mediumStatistics.Status & EChunkStatus::Lost)) {
            mediaOnWhichLost.push_back(mediumIndex);
        } else {
            mediaOnWhichPresent.push_back(mediumIndex);
            precarious = precarious && mediumTransient;
        }
    }

    // Intra-medium replication has been dealt above.
    // The only cross-medium thing left do is to kickstart replication of chunks
    // lost on one medium but not on another.
    ComputeRegularChunkStatisticsCrossMedia(
        result,
        precarious,
        allMediaTransient,
        mediaOnWhichLost,
        mediaOnWhichPresent.size(),
        mediaOnWhichUnderreplicatedCount);

    return result;
}

void TChunkReplicator::ComputeRegularChunkStatisticsForMedium(
    TPerMediumChunkStatistics& result,
    int replicationFactor,
    int replicaCount,
    int decommissionedReplicaCount,
    const TNodePtrWithIndexesList& decommissionedReplicas,
    bool hasUnsafelyPlacedReplicas)
{
    result.ReplicaCount[GenericChunkReplicaIndex] = replicaCount;
    result.DecommissionedReplicaCount[GenericChunkReplicaIndex] = decommissionedReplicaCount;

    if (replicaCount + decommissionedReplicaCount == 0) {
        result.Status |= EChunkStatus::Lost;
    }

    if (replicaCount < replicationFactor && replicaCount + decommissionedReplicaCount > 0) {
        result.Status |= EChunkStatus::Underreplicated;
    }

    if (decommissionedReplicaCount > 0 && replicaCount + decommissionedReplicaCount > replicationFactor) {
        result.Status |= EChunkStatus::Overreplicated;
        result.DecommissionedRemovalReplicas.append(decommissionedReplicas.begin(), decommissionedReplicas.end());
    } else if (replicaCount > replicationFactor) {
        result.Status |= EChunkStatus::Overreplicated;
        result.BalancingRemovalIndexes.push_back(GenericChunkReplicaIndex);
    }

    if (replicationFactor > 1 && hasUnsafelyPlacedReplicas && None(result.Status & EChunkStatus::Overreplicated)) {
        result.Status |= EChunkStatus::UnsafelyPlaced;
    }

    if (Any(result.Status & (EChunkStatus::Underreplicated | EChunkStatus::UnsafelyPlaced)) &&
        replicaCount + decommissionedReplicaCount > 0)
    {
        result.ReplicationIndexes.push_back(GenericChunkReplicaIndex);
    }
}

void TChunkReplicator::ComputeRegularChunkStatisticsCrossMedia(
    TChunkStatistics& result,
    bool precarious,
    bool allMediaTransient,
    const SmallVector<int, MaxMediumCount>& mediaOnWhichLost,
    int mediaOnWhichPresentCount,
    int mediaOnWhichUnderreplicatedCount)
{
    if (mediaOnWhichPresentCount == 0) {
        result.Status |= ECrossMediumChunkStatus::Lost;
    }
    if (precarious && !allMediaTransient) {
        result.Status |= ECrossMediumChunkStatus::Precarious;
    }

    if (!mediaOnWhichLost.empty() && mediaOnWhichPresentCount > 0) {
        for (auto mediumIndex : mediaOnWhichLost) {
            auto& mediumStatistics = result.PerMediumStatistics[mediumIndex];
            mediumStatistics.Status |= EChunkStatus::Underreplicated;
            mediumStatistics.ReplicationIndexes.push_back(GenericChunkReplicaIndex);
        }
        result.Status |= ECrossMediumChunkStatus::MediumWiseLost;
    } else if (mediaOnWhichUnderreplicatedCount > 0) {
        result.Status |= ECrossMediumChunkStatus::Deficient;
    }
}

TChunkReplicator::TChunkStatistics TChunkReplicator::ComputeErasureChunkStatistics(TChunk* chunk)
{
    TChunkStatistics result;

    auto* codec = NErasure::GetCodec(chunk->GetErasureCodec());

    TMediumMap<std::array<TNodePtrWithIndexesList, ChunkReplicaIndexBound>> decommissionedReplicas;
    TMediumMap<std::array<ui8, RackIndexBound>> perRackReplicaCounters;
    // An arbitrary replica collocated with too may others within a single rack - per medium.
    TMediumIntMap unsafelyPlacedReplicaIndexes;

    TMediumIntMap totalReplicaCounts;
    TMediumIntMap totalDecommissionedReplicaCounts;

    auto mark = TNode::GenerateVisitMark();

    const auto chunkReplication = GetChunkAggregatedReplication(chunk);
    for (const auto& entry : chunkReplication) {
        auto mediumIndex = entry.GetMediumIndex();

        unsafelyPlacedReplicaIndexes[mediumIndex] = -1;
        totalReplicaCounts[mediumIndex] = 0;
        totalDecommissionedReplicaCounts[mediumIndex] = 0;
    }

    for (auto replica : chunk->StoredReplicas()) {
        auto* node = replica.GetPtr();
        int replicaIndex = replica.GetReplicaIndex();
        int mediumIndex = replica.GetMediumIndex();
        auto& mediumStatistics = result.PerMediumStatistics[mediumIndex];

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
        const auto* rack = node->GetRack();
        if (rack) {
            int rackIndex = rack->GetIndex();
            int maxReplicasPerRack = ChunkPlacement_->GetMaxReplicasPerRack(mediumIndex, chunk);
            if (++perRackReplicaCounters[mediumIndex][rackIndex] > maxReplicasPerRack) {
                // A erasure chunk is considered placed unsafely if some non-null rack
                // contains more replicas than returned by TChunk::GetMaxReplicasPerRack.
                unsafelyPlacedReplicaIndexes[mediumIndex] = replicaIndex;
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

        const auto replicationPolicy = entry.Policy();

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
            mediumReplicationFactor,
            decommissionedReplicas[mediumIndex],
            unsafelyPlacedReplicaIndexes[mediumIndex],
            mediumToErasedIndexes[mediumIndex],
            dataPartsOnly);
    }

    ComputeErasureChunkStatisticsCrossMedia(
        result,
        codec,
        allMediaTransient,
        allMediaDataPartsOnly,
        mediumToErasedIndexes,
        activeMedia);

    return result;
}

void TChunkReplicator::ComputeErasureChunkStatisticsForMedium(
    TPerMediumChunkStatistics& result,
    NErasure::ICodec* codec,
    int replicationFactor,
    std::array<TNodePtrWithIndexesList, ChunkReplicaIndexBound>& decommissionedReplicas,
    int unsafelyPlacedReplicaIndex,
    NErasure::TPartIndexSet& erasedIndexes,
    bool dataPartsOnly)
{
    YT_ASSERT(0 <= replicationFactor && replicationFactor <= 1);

    int totalPartCount = codec->GetTotalPartCount();
    int dataPartCount = codec->GetDataPartCount();

    for (int index = 0; index < totalPartCount; ++index) {
        int replicaCount = result.ReplicaCount[index];
        int decommissionedReplicaCount = result.DecommissionedReplicaCount[index];
        auto isDataPart = index < dataPartCount;
        auto removalAdvised = replicationFactor == 0 || (!isDataPart && dataPartsOnly);
        auto targetReplicationFactor = removalAdvised ? 0 : 1;

        if (replicaCount >= targetReplicationFactor && decommissionedReplicaCount > 0) {
            result.Status |= EChunkStatus::Overreplicated;
            const auto& replicas = decommissionedReplicas[index];
            result.DecommissionedRemovalReplicas.append(replicas.begin(), replicas.end());
        }

        if (replicaCount > targetReplicationFactor && decommissionedReplicaCount == 0) {
            result.Status |= EChunkStatus::Overreplicated;
            result.BalancingRemovalIndexes.push_back(index);
        }

        if (replicaCount == 0 && decommissionedReplicaCount > 0 && !removalAdvised && decommissionedReplicas.size() > index) {
            const auto& replicas = decommissionedReplicas[index];
            // A replica may be "decommissioned" either because it's node is
            // decommissioned or that node holds another part of the chunk (and that's
            // not allowed by the configuration). Let's distinguish these cases.
            auto isReplicaDecommissioned = [&] (const TNodePtrWithIndexes& replica) {
                return IsReplicaDecommissioned(replica);
            };
            if (std::all_of(replicas.begin(), replicas.end(), isReplicaDecommissioned)) {
                result.Status |= isDataPart
                    ? EChunkStatus::DataDecommissioned
                    : EChunkStatus::ParityDecommissioned;
            } else {
                result.Status |= EChunkStatus::Underreplicated;
                result.ReplicationIndexes.push_back(index);
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

    if (unsafelyPlacedReplicaIndex != -1 && None(result.Status & EChunkStatus::Overreplicated)) {
        result.Status |= EChunkStatus::UnsafelyPlaced;
        if (result.ReplicationIndexes.empty()) {
            result.ReplicationIndexes.push_back(unsafelyPlacedReplicaIndex);
        }
    }
}

void TChunkReplicator::ComputeErasureChunkStatisticsCrossMedia(
    TChunkStatistics& result,
    NErasure::ICodec* codec,
    bool allMediaTransient,
    bool allMediaDataPartsOnly,
    const TMediumMap<NErasure::TPartIndexSet>& mediumToErasedIndexes,
    const TMediumSet& activeMedia)
{
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

TChunkReplicator::TChunkStatistics TChunkReplicator::ComputeJournalChunkStatistics(TChunk* chunk)
{
    TChunkStatistics results;

    const auto replication = GetChunkAggregatedReplication(chunk);

    TMediumIntMap replicaCount{};
    int totalReplicaCount = 0;
    TMediumIntMap decommissionedReplicaCount{};
    int totalDecommissionedReplicaCount = 0;
    TMediumIntMap sealedReplicaCount{};
    int totalSealedReplicaCount = 0;
    TMediumIntMap unsealedReplicaCount{};
    TMediumMap<TNodePtrWithIndexesList> decommissionedReplicas{};
    TMediumMap<std::array<ui8, RackIndexBound>> perRackReplicaCounters{};
    TMediumMap<bool> hasUnsafelyPlacedReplicas{};

    for (auto replica : chunk->StoredReplicas()) {
        const auto mediumIndex = replica.GetMediumIndex();

        if (replica.GetReplicaIndex() == SealedChunkReplicaIndex) {
            ++sealedReplicaCount[mediumIndex];
            ++totalSealedReplicaCount;
        } else {
            ++unsealedReplicaCount[mediumIndex];
        }
        if (IsReplicaDecommissioned(replica)) {
            ++decommissionedReplicaCount[mediumIndex];
            decommissionedReplicas[mediumIndex].push_back(replica);
            ++totalDecommissionedReplicaCount;
        } else {
            ++replicaCount[mediumIndex];
            ++totalReplicaCount;
        }
        const auto* rack = replica.GetPtr()->GetRack();
        if (rack) {
            int rackIndex = rack->GetIndex();
            int maxReplicasPerRack = ChunkPlacement_->GetMaxReplicasPerRack(mediumIndex, chunk, std::nullopt);
            if (++perRackReplicaCounters[mediumIndex][rackIndex] > maxReplicasPerRack) {
                // A journal chunk is considered placed unsafely if some non-null rack
                // contains more replicas than returned by TChunk::GetMaxReplicasPerRack.
                hasUnsafelyPlacedReplicas[mediumIndex] = true;
            }
        }
    }

    const bool isSealed = chunk->IsSealed();
    const int readQuorum = chunk->GetReadQuorum();

    bool precarious = true;
    bool allMediaTransient = true;
    SmallVector<int, MaxMediumCount> mediaOnWhichLost;
    int mediaOnWhichPresentCount = 0;
    int mediaOnWhichUnderreplicatedCount = 0;
    int mediaOnWhichSealedMissingCount = 0;

    const auto& chunkManager = Bootstrap_->GetChunkManager();

    for (const auto& entry : replication) {
        auto mediumIndex = entry.GetMediumIndex();
        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        YT_VERIFY(IsObjectAlive(medium));

        if (medium->GetCache()) {
            continue;
        }

        auto& mediumStatistics = results.PerMediumStatistics[mediumIndex];
        auto mediumTransient = medium->GetTransient();

        const auto mediumReplicationPolicy = entry.Policy();
        auto mediumReplicaCount = replicaCount[mediumIndex];
        auto mediumDecommissionedReplicaCount = decommissionedReplicaCount[mediumIndex];
        if (!mediumReplicationPolicy &&
            mediumReplicaCount == 0 &&
            mediumDecommissionedReplicaCount == 0)
        {
            // This medium is irrelevant to this chunk.
            continue;
        }

        ComputeJournalChunkStatisticsForMedium(
            mediumStatistics,
            mediumReplicationPolicy,
            mediumReplicaCount,
            mediumDecommissionedReplicaCount,
            decommissionedReplicas[mediumIndex],
            sealedReplicaCount[mediumIndex],
            unsealedReplicaCount[mediumIndex],
            hasUnsafelyPlacedReplicas[mediumIndex],
            isSealed,
            readQuorum);

        allMediaTransient = allMediaTransient && mediumTransient;

        if (Any(mediumStatistics.Status & EChunkStatus::Underreplicated)) {
            ++mediaOnWhichUnderreplicatedCount;
        }

        if (Any(mediumStatistics.Status & EChunkStatus::SealedMissing)) {
            ++mediaOnWhichSealedMissingCount;
        }

        if (Any(mediumStatistics.Status & EChunkStatus::Lost)) {
            mediaOnWhichLost.push_back(mediumIndex);
        } else {
            ++mediaOnWhichPresentCount;
            precarious = precarious && mediumTransient;
        }
    }

    ComputeJournalChunkStatisticsCrossMedia(
        results,
        totalReplicaCount,
        totalDecommissionedReplicaCount,
        totalSealedReplicaCount,
        precarious,
        allMediaTransient,
        mediaOnWhichLost,
        mediaOnWhichPresentCount,
        mediaOnWhichUnderreplicatedCount,
        mediaOnWhichSealedMissingCount,
        readQuorum);

    return results;
}

void TChunkReplicator::ComputeJournalChunkStatisticsForMedium(
    TPerMediumChunkStatistics& result,
    TReplicationPolicy replicationPolicy,
    int replicaCount,
    int decommissionedReplicaCount,
    const TNodePtrWithIndexesList& decommissionedReplicas,
    int sealedReplicaCount,
    int unsealedReplicaCount,
    bool hasUnsafelyPlacedReplicas,
    bool isSealed,
    int readQuorum)
{
    result.ReplicaCount[GenericChunkReplicaIndex] = replicaCount;
    result.DecommissionedReplicaCount[GenericChunkReplicaIndex] = decommissionedReplicaCount;

    if (replicaCount + decommissionedReplicaCount == 0) {
        result.Status |= EChunkStatus::Lost;
    }

    if (isSealed) {
        result.Status |= EChunkStatus::Sealed;

        const auto replicationFactor = replicationPolicy.GetReplicationFactor();

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

        if (replicationFactor > 0 && sealedReplicaCount == 0) {
            result.Status |= EChunkStatus::SealedMissing;
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
}

void TChunkReplicator::ComputeJournalChunkStatisticsCrossMedia(
    TChunkStatistics& result,
    int totalReplicaCount,
    int totalDecommissionedReplicaCount,
    int totalSealedReplicaCount,
    bool precarious,
    bool allMediaTransient,
    const SmallVector<int, MaxMediumCount>& mediaOnWhichLost,
    int mediaOnWhichPresentCount,
    int mediaOnWhichUnderreplicatedCount,
    int mediaOnWhichSealedMissingCount,
    int readQuorum)
{
    if (totalReplicaCount + totalDecommissionedReplicaCount < readQuorum && totalSealedReplicaCount == 0) {
        result.Status |= ECrossMediumChunkStatus::QuorumMissing;
    }

    if (mediaOnWhichPresentCount == 0) {
        result.Status |= ECrossMediumChunkStatus::Lost;
    }
    if (precarious && !allMediaTransient) {
        result.Status |= ECrossMediumChunkStatus::Precarious;
    }

    if (!mediaOnWhichLost.empty() && mediaOnWhichPresentCount > 0) {
        if (totalSealedReplicaCount > 0) {
            for (auto mediumIndex : mediaOnWhichLost) {
                auto& mediumStatistics = result.PerMediumStatistics[mediumIndex];
                mediumStatistics.Status |= EChunkStatus::Underreplicated;
                mediumStatistics.ReplicationIndexes.push_back(GenericChunkReplicaIndex);
            }
        }
        result.Status |= ECrossMediumChunkStatus::MediumWiseLost;
    } else if (mediaOnWhichUnderreplicatedCount > 0 || mediaOnWhichSealedMissingCount > 0) {
        result.Status |= ECrossMediumChunkStatus::Deficient;
    }
}

void TChunkReplicator::ScheduleJobs(
    TNode* node,
    const TNodeResources& resourceUsage,
    const TNodeResources& resourceLimits,
    const std::vector<TJobPtr>& runningJobs,
    std::vector<TJobPtr>* jobsToStart,
    std::vector<TJobPtr>* jobsToAbort,
    std::vector<TJobPtr>* jobsToRemove)
{
    UpdateInterDCEdgeCapacities(); // Pull capacity changes.

    ProcessExistingJobs(
        node,
        runningJobs,
        jobsToAbort,
        jobsToRemove);

    ScheduleNewJobs(
        node,
        resourceUsage,
        resourceLimits,
        jobsToStart);
}

void TChunkReplicator::OnNodeUnregistered(TNode* node)
{
    auto idToJob = node->IdToJob();
    for (const auto& pair : idToJob) {
        const auto& job = pair.second;
        YT_LOG_DEBUG("Job canceled (JobId: %v)", job->GetJobId());
        UnregisterJob(job);
    }
    node->Reset();
}

void TChunkReplicator::OnNodeDisposed(TNode* node)
{
    YT_VERIFY(node->IdToJob().empty());
    YT_VERIFY(node->ChunkSealQueue().empty());
    YT_VERIFY(node->ChunkRemovalQueue().empty());
    for (const auto& queue : node->ChunkReplicationQueues()) {
        YT_VERIFY(queue.empty());
    }
}

void TChunkReplicator::OnChunkDestroyed(TChunk* chunk)
{
    RefreshScanner_->OnChunkDestroyed(chunk);
    RequisitionUpdateScanner_->OnChunkDestroyed(chunk);
    ResetChunkStatus(chunk);
    RemoveChunkFromQueuesOnDestroy(chunk);
    CancelChunkJobs(chunk);
}

void TChunkReplicator::OnReplicaRemoved(
    TNode* node,
    TChunkPtrWithIndexes chunkWithIndexes,
    ERemoveReplicaReason reason)
{
    const auto* chunk = chunkWithIndexes.GetPtr();
    TChunkIdWithIndexes chunkIdWithIndexes(
        chunk->GetId(),
        chunkWithIndexes.GetReplicaIndex(),
        chunkWithIndexes.GetMediumIndex());
    node->RemoveFromChunkReplicationQueues(chunkWithIndexes, AllMediaIndex);
    if (reason != ERemoveReplicaReason::ChunkDestroyed) {
        node->RemoveFromChunkRemovalQueue(chunkIdWithIndexes);
    }
    if (chunk->IsJournal()) {
        node->RemoveFromChunkSealQueue(chunkWithIndexes);
    }
}

void TChunkReplicator::ScheduleUnknownReplicaRemoval(
    TNode* node,
    const TChunkIdWithIndexes& chunkIdWithIndexes)
{
    node->AddToChunkRemovalQueue(chunkIdWithIndexes);
}

void TChunkReplicator::ScheduleReplicaRemoval(
    TNode* node,
    TChunkPtrWithIndexes chunkWithIndexes)
{
    TChunkIdWithIndexes chunkIdWithIndexes(
        chunkWithIndexes.GetPtr()->GetId(),
        chunkWithIndexes.GetReplicaIndex(),
        chunkWithIndexes.GetMediumIndex());
    node->AddToChunkRemovalQueue(chunkIdWithIndexes);
}

void TChunkReplicator::ProcessExistingJobs(
    TNode* node,
    const std::vector<TJobPtr>& currentJobs,
    std::vector<TJobPtr>* jobsToAbort,
    std::vector<TJobPtr>* jobsToRemove)
{
    const auto& address = node->GetDefaultAddress();

    for (const auto& job : currentJobs) {
        auto jobId = job->GetJobId();
        auto jobType = job->GetType();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(CellTagFromId(jobId) == multicellManager->GetCellTag());

        YT_VERIFY(TypeFromId(jobId) == EObjectType::MasterJob);

        switch (job->GetState()) {
            case EJobState::Running:
            case EJobState::Waiting: {
                if (TInstant::Now() - job->GetStartTime() > GetDynamicConfig()->JobTimeout) {
                    jobsToAbort->push_back(job);
                    YT_LOG_WARNING("Job timed out (JobId: %v, JobType: %v, Address: %v, Duration: %v)",
                        jobId,
                        jobType,
                        address,
                        TInstant::Now() - job->GetStartTime());
                    break;
                }

                switch (job->GetState()) {
                    case EJobState::Running:
                        YT_LOG_DEBUG("Job is running (JobId: %v, JobType: %v, Address: %v)",
                            jobId,
                            jobType,
                            address);
                        break;

                    case EJobState::Waiting:
                        YT_LOG_DEBUG("Job is waiting (JobId: %v, JobType: %v, Address: %v)",
                            jobId,
                            jobType,
                            address);
                        break;

                    default:
                        YT_ABORT();
                }
                break;
            }

            case EJobState::Completed:
            case EJobState::Failed:
            case EJobState::Aborted: {
                jobsToRemove->push_back(job);
                auto rescheduleChunkRemoval = [&] {
                    if (jobType == EJobType::RemoveChunk &&
                        !job->Error().FindMatching(NChunkClient::EErrorCode::NoSuchChunk))
                    {
                        const auto& replica = job->GetChunkIdWithIndexes();
                        node->AddToChunkRemovalQueue(replica);
                    }
                };

                switch (job->GetState()) {
                    case EJobState::Completed:
                        YT_LOG_DEBUG("Job completed (JobId: %v, JobType: %v, Address: %v)",
                            jobId,
                            jobType,
                            address);
                        break;

                    case EJobState::Failed:
                        YT_LOG_WARNING(job->Error(), "Job failed (JobId: %v, JobType: %v, Address: %v)",
                            jobId,
                            jobType,
                            address);
                        rescheduleChunkRemoval();
                        break;

                    case EJobState::Aborted:
                        YT_LOG_WARNING(job->Error(), "Job aborted (JobId: %v, JobType: %v, Address: %v)",
                            jobId,
                            jobType,
                            address);
                        rescheduleChunkRemoval();
                        break;

                    default:
                        YT_ABORT();
                }
                UnregisterJob(job);
                break;
            }

            default:
                YT_ABORT();
        }
    }

    // Check for missing jobs
    THashSet<TJobPtr> currentJobSet(currentJobs.begin(), currentJobs.end());
    std::vector<TJobPtr> missingJobs;
    for (const auto& pair : node->IdToJob()) {
        const auto& job = pair.second;
        if (currentJobSet.find(job) == currentJobSet.end()) {
            missingJobs.push_back(job);
            YT_LOG_WARNING("Job is missing (JobId: %v, JobType: %v, Address: %v)",
                job->GetJobId(),
                job->GetType(),
                address);
        }
    }

    for (const auto& job : missingJobs) {
        UnregisterJob(job);
    }
}

TJobId TChunkReplicator::GenerateJobId()
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    return MakeRandomId(EObjectType::MasterJob, multicellManager->GetCellTag());
}

bool TChunkReplicator::CreateReplicationJob(
    TNode* sourceNode,
    TChunkPtrWithIndexes chunkWithIndexes,
    TMedium* targetMedium,
    TJobPtr* job)
{
    auto* chunk = chunkWithIndexes.GetPtr();
    auto replicaIndex = chunkWithIndexes.GetReplicaIndex();

    if (!IsObjectAlive(chunk)) {
        return true;
    }

    const auto& objectManager = Bootstrap_->GetObjectManager();
    if (chunk->GetScanFlag(EChunkScanKind::Refresh, objectManager->GetCurrentEpoch())) {
        return true;
    }

    if (chunk->IsJobScheduled()) {
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
    } else if (Any(mediumStatistics.Status & EChunkStatus::UnsafelyPlaced)) {
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
        replicasNeeded,
        1,
        std::nullopt,
        UnsaturatedInterDCEdges_[sourceNode->GetDataCenter()],
        ESessionType::Replication);
    if (targetNodes.empty()) {
        return false;
    }

    TNodePtrWithIndexesList targetReplicas;
    for (auto* node : targetNodes) {
        targetReplicas.emplace_back(node, replicaIndex, targetMediumIndex);
    }

    *job = TJob::CreateReplicate(
        GenerateJobId(),
        chunkWithIndexes,
        sourceNode,
        targetReplicas);

    YT_LOG_DEBUG("Replication job scheduled (JobId: %v, Address: %v, ChunkId: %v, TargetAddresses: %v)",
        (*job)->GetJobId(),
        sourceNode->GetDefaultAddress(),
        chunkWithIndexes,
        MakeFormattableView(targetNodes, TNodePtrAddressFormatter()));

    return targetNodes.size() == replicasNeeded;
}

bool TChunkReplicator::CreateBalancingJob(
    TNode* sourceNode,
    TChunkPtrWithIndexes chunkWithIndexes,
    double maxFillFactor,
    TJobPtr* job)
{
    auto* chunk = chunkWithIndexes.GetPtr();

    const auto& objectManager = Bootstrap_->GetObjectManager();
    if (chunk->GetScanFlag(EChunkScanKind::Refresh, objectManager->GetCurrentEpoch())) {
        return true;
    }

    if (chunk->IsJobScheduled()) {
        return true;
    }

    auto replicaIndex = chunkWithIndexes.GetReplicaIndex();
    auto mediumIndex = chunkWithIndexes.GetMediumIndex();

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto* medium = chunkManager->GetMediumByIndex(mediumIndex);

    auto* targetNode = ChunkPlacement_->AllocateBalancingTarget(
        medium,
        chunk,
        maxFillFactor,
        UnsaturatedInterDCEdges_[sourceNode->GetDataCenter()]);
    if (!targetNode) {
        return false;
    }

    TNodePtrWithIndexesList targetReplicas{
        TNodePtrWithIndexes(targetNode, replicaIndex, mediumIndex)
    };

    *job = TJob::CreateReplicate(
        GenerateJobId(),
        chunkWithIndexes,
        sourceNode,
        targetReplicas);

    YT_LOG_DEBUG("Balancing job scheduled (JobId: %v, Address: %v, ChunkId: %v, TargetAddress: %v)",
        (*job)->GetJobId(),
        sourceNode->GetDefaultAddress(),
        chunkWithIndexes,
        targetNode->GetDefaultAddress());

    return true;
}

bool TChunkReplicator::CreateRemovalJob(
    TNode* node,
    const TChunkIdWithIndexes& chunkIdWithIndexes,
    TJobPtr* job)
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    const auto& objectManager = Bootstrap_->GetObjectManager();

    auto* chunk = chunkManager->FindChunk(chunkIdWithIndexes.Id);
    // NB: Allow more than one job for dead chunks.
    if (IsObjectAlive(chunk)) {
        if (chunk->GetScanFlag(EChunkScanKind::Refresh, objectManager->GetCurrentEpoch())) {
            return true;
        }
        if (chunk->IsJobScheduled()) {
            return true;
        }
    }

    *job = TJob::CreateRemove(
        GenerateJobId(),
        chunkIdWithIndexes,
        node);

    YT_LOG_DEBUG("Removal job scheduled (JobId: %v, Address: %v, ChunkId: %v)",
        (*job)->GetJobId(),
        node->GetDefaultAddress(),
        chunkIdWithIndexes);

    return true;
}

bool TChunkReplicator::CreateRepairJob(
    EChunkRepairQueue repairQueue,
    TNode* node,
    TChunkPtrWithIndexes chunkWithIndexes,
    TJobPtr* job)
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

    const auto& objectManager = Bootstrap_->GetObjectManager();
    if (chunk->GetScanFlag(EChunkScanKind::Refresh, objectManager->GetCurrentEpoch())) {
        return true;
    }

    if (chunk->IsJobScheduled()) {
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

    auto erasedPartCount = static_cast<int>(erasedPartIndexes.size());

    auto targetNodes = ChunkPlacement_->AllocateWriteTargets(
        medium,
        chunk,
        erasedPartCount,
        erasedPartCount,
        std::nullopt,
        UnsaturatedInterDCEdges_[node->GetDataCenter()],
        ESessionType::Repair);
    if (targetNodes.empty()) {
        return false;
    }

    YT_VERIFY(targetNodes.size() == erasedPartCount);

    TNodePtrWithIndexesList targetReplicas;
    int targetIndex = 0;
    for (auto* node : targetNodes) {
        targetReplicas.emplace_back(node, erasedPartIndexes[targetIndex++], mediumIndex);
    }

    *job = TJob::CreateRepair(
        GenerateJobId(),
        chunk,
        node,
        targetReplicas,
        GetDynamicConfig()->RepairJobMemoryUsage,
        repairQueue == EChunkRepairQueue::Decommissioned);

    YT_LOG_DEBUG("Repair job scheduled (JobId: %v, Address: %v, ChunkId: %v, Targets: %v, ErasedPartIndexes: %v)",
        (*job)->GetJobId(),
        node->GetDefaultAddress(),
        chunkWithIndexes,
        MakeFormattableView(targetNodes, TNodePtrAddressFormatter()),
        erasedPartIndexes);

    return true;
}

bool TChunkReplicator::CreateSealJob(
    TNode* node,
    TChunkPtrWithIndexes chunkWithIndexes,
    TJobPtr* job)
{
    YT_VERIFY(chunkWithIndexes.GetReplicaIndex() == GenericChunkReplicaIndex);

    auto* chunk = chunkWithIndexes.GetPtr();
    YT_VERIFY(chunk->IsJournal());
    YT_VERIFY(chunk->IsSealed());

    if (!IsObjectAlive(chunk)) {
        return true;
    }

    if (chunk->IsJobScheduled()) {
        return true;
    }

    // NB: Seal jobs can be started even if chunk refresh is scheduled.

    if (chunk->StoredReplicas().size() < chunk->GetReadQuorum()) {
        return true;
    }

    *job = TJob::CreateSeal(
        GenerateJobId(),
        chunkWithIndexes,
        node);

    YT_LOG_DEBUG("Seal job scheduled (JobId: %v, Address: %v, ChunkId: %v)",
        (*job)->GetJobId(),
        node->GetDefaultAddress(),
        chunkWithIndexes);

    return true;
}

void TChunkReplicator::ScheduleNewJobs(
    TNode* node,
    TNodeResources resourceUsage,
    TNodeResources resourceLimits,
    std::vector<TJobPtr>* jobsToStart)
{
    if (JobThrottler_->IsOverdraft()) {
        return;
    }

    const auto& resourceLimitsOverrides = node->ResourceLimitsOverrides();
    #define XX(name, Name) \
        if (resourceLimitsOverrides.has_##name()) { \
            resourceLimits.set_##name(std::min(resourceLimitsOverrides.name(), resourceLimits.name())); \
        }
    ITERATE_NODE_RESOURCE_LIMITS_OVERRIDES(XX)
    #undef XX

    const auto* nodeDataCenter = node->GetDataCenter();

    auto registerJob = [&] (const TJobPtr& job) {
        if (job) {
            resourceUsage += job->ResourceUsage();
            jobsToStart->push_back(job);
            RegisterJob(job);
            JobThrottler_->Acquire(1);
        }
    };

    int misscheduledReplicationJobs = 0;
    int misscheduledRepairJobs = 0;
    int misscheduledSealJobs = 0;
    int misscheduledRemovalJobs = 0;

    // NB: Beware of chunks larger than the limit; we still need to be able to replicate them one by one.
    auto hasSpareReplicationResources = [&] () {
        return
            misscheduledReplicationJobs < GetDynamicConfig()->MaxMisscheduledReplicationJobsPerHeartbeat &&
            resourceUsage.replication_slots() < resourceLimits.replication_slots() &&
            (resourceUsage.replication_slots() == 0 || resourceUsage.replication_data_size() < resourceLimits.replication_data_size());
    };

    // NB: Beware of chunks larger than the limit; we still need to be able to repair them one by one.
    auto hasSpareRepairResources = [&] () {
        return
            misscheduledRepairJobs < GetDynamicConfig()->MaxMisscheduledRepairJobsPerHeartbeat &&
            resourceUsage.repair_slots() < resourceLimits.repair_slots() &&
            (resourceUsage.repair_slots() == 0 || resourceUsage.repair_data_size() < resourceLimits.repair_data_size());
    };

    auto hasSpareSealResources = [&] () {
        return
            misscheduledSealJobs < GetDynamicConfig()->MaxMisscheduledSealJobsPerHeartbeat &&
            resourceUsage.seal_slots() < resourceLimits.seal_slots();
    };

    auto hasSpareRemovalResources = [&] () {
        return
            misscheduledRemovalJobs < GetDynamicConfig()->MaxMisscheduledRemovalJobsPerHeartbeat &&
            resourceUsage.removal_slots() < resourceLimits.removal_slots();
    };

    if (IsReplicatorEnabled()) {
        const auto& chunkManager = Bootstrap_->GetChunkManager();

        // Schedule replication jobs.
        for (auto& queue : node->ChunkReplicationQueues()) {
            auto it = queue.begin();
            while (it != queue.end() &&
                   hasSpareReplicationResources() &&
                   HasUnsaturatedInterDCEdgeStartingFrom(nodeDataCenter))
            {
                auto jt = it++;
                auto chunkWithIndexes = jt->first;
                auto& mediumIndexSet = jt->second;
                for (int mediumIndex = 0; mediumIndex < mediumIndexSet.size(); ++mediumIndex) {
                    if (mediumIndexSet.test(mediumIndex)) {
                        TJobPtr job;
                        auto* medium = chunkManager->GetMediumByIndex(mediumIndex);
                        if (CreateReplicationJob(node, chunkWithIndexes, medium, &job)) {
                            mediumIndexSet.reset(mediumIndex);
                        } else {
                            ++misscheduledReplicationJobs;
                        }
                        registerJob(std::move(job));
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

            while (hasSpareRepairResources() &&
                   HasUnsaturatedInterDCEdgeStartingFrom(nodeDataCenter))
            {
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
                TJobPtr job;
                if (CreateRepairJob(queue, node, chunkWithIndexes, &job)) {
                    chunk->SetRepairQueueIterator(chunkWithIndexes.GetMediumIndex(), queue, TChunkRepairQueueIterator());
                    chunkRepairQueue.erase(chunkIt);
                    if (job) {
                        ChunkRepairQueueBalancer(queue).AddWeight(
                            *winner,
                            job->ResourceUsage().repair_data_size() * job->TargetReplicas().size());
                    }
                } else {
                    ++misscheduledRepairJobs;
                }
                registerJob(std::move(job));
            }
        }

        // Schedule removal jobs.
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
                for (int mediumIndex = 0; mediumIndex < mediumIndexSet.size(); ++mediumIndex) {
                    if (mediumIndexSet.test(mediumIndex)) {
                        TChunkIdWithIndexes chunkIdWithIndexes(
                            chunkIdWithIndex.Id,
                            chunkIdWithIndex.ReplicaIndex,
                            mediumIndex);
                        TJobPtr job;
                        if (CreateRemovalJob(node, chunkIdWithIndexes, &job)) {
                            mediumIndexSet.reset(mediumIndex);
                        } else {
                            ++misscheduledRemovalJobs;
                        }
                        registerJob(std::move(job));
                    }
                }
                if (mediumIndexSet.none()) {
                    queue.erase(jt);
                }
            }
        }

        // Schedule balancing jobs.
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
                HasUnsaturatedInterDCEdgeStartingFrom(nodeDataCenter) &&
                ChunkPlacement_->HasBalancingTargets(
                    UnsaturatedInterDCEdges_[node->GetDataCenter()],
                    medium,
                    targetFillFactor))
            {
                int maxJobs = std::max(0, resourceLimits.replication_slots() - resourceUsage.replication_slots());
                auto chunksToBalance = ChunkPlacement_->GetBalancingChunks(medium, node, maxJobs);
                for (auto chunkWithIndexes : chunksToBalance) {
                    if (!hasSpareReplicationResources()) {
                        break;
                    }

                    TJobPtr job;
                    if (!CreateBalancingJob(node, chunkWithIndexes, targetFillFactor, &job)) {
                        ++misscheduledReplicationJobs;
                    }
                    registerJob(std::move(job));
                }
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
            auto* chunk = jt->first;
            auto& mediumIndexSet = jt->second;
            for (int mediumIndex = 0; mediumIndex < mediumIndexSet.size(); ++mediumIndex) {
                if (mediumIndexSet.test(mediumIndex)) {
                    TChunkPtrWithIndexes chunkWithIndexes(
                        chunk,
                        GenericChunkReplicaIndex,
                        mediumIndex);
                    TJobPtr job;
                    if (CreateSealJob(node, chunkWithIndexes, &job)) {
                        mediumIndexSet.reset(mediumIndex);
                    } else {
                        ++misscheduledRepairJobs;
                    }
                    registerJob(std::move(job));
                }
            }
            if (mediumIndexSet.none()) {
                queue.erase(jt);
            }
        }
    }
}

void TChunkReplicator::RefreshChunk(TChunk* chunk)
{
    if (!chunk->IsConfirmed()) {
        return;
    }

    if (chunk->IsForeign()) {
        return;
    }

    const auto replication = GetChunkAggregatedReplication(chunk);

    ResetChunkStatus(chunk);
    RemoveChunkFromQueuesOnRefresh(chunk);

    auto allMediaStatistics = ComputeChunkStatistics(chunk);

    auto durabilityRequired = false;

    const auto& chunkManager = Bootstrap_->GetChunkManager();

    for (const auto& entry : replication) {
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

        if (!chunk->IsJobScheduled()) {
            if (Any(statistics.Status & EChunkStatus::Overreplicated) &&
                None(allMediaStatistics.Status & (ECrossMediumChunkStatus::Deficient | ECrossMediumChunkStatus::MediumWiseLost)))
            {
                for (auto nodeWithIndexes : statistics.DecommissionedRemovalReplicas) {
                    YT_ASSERT(mediumIndex == nodeWithIndexes.GetMediumIndex());
                    int replicaIndex = nodeWithIndexes.GetReplicaIndex();
                    TChunkIdWithIndexes chunkIdWithIndexes(chunk->GetId(), replicaIndex, mediumIndex);
                    auto* node = nodeWithIndexes.GetPtr();
                    if (node->GetLocalState() == ENodeState::Online) {
                        node->AddToChunkRemovalQueue(chunkIdWithIndexes);
                    }
                }

                for (int replicaIndex : statistics.BalancingRemovalIndexes) {
                    TChunkPtrWithIndexes chunkWithIndexes(chunk, replicaIndex, mediumIndex);
                    TChunkIdWithIndexes chunkIdWithIndexes(chunk->GetId(), replicaIndex, mediumIndex);
                    auto* targetNode = ChunkPlacement_->GetRemovalTarget(chunkWithIndexes);
                    if (targetNode) {
                        targetNode->AddToChunkRemovalQueue(chunkIdWithIndexes);
                    }
                }
            }

            // This check may yield true even for lost chunks when cross-medium replication is in progress.
            if (Any(statistics.Status & (EChunkStatus::Underreplicated | EChunkStatus::UnsafelyPlaced))) {
                for (auto replicaIndex : statistics.ReplicationIndexes) {
                    // Cap replica count minus one against the range [0, ReplicationPriorityCount - 1].
                    int replicaCount = statistics.ReplicaCount[replicaIndex];
                    int priority = std::max(std::min(replicaCount - 1, ReplicationPriorityCount - 1), 0);

                    for (auto replica : chunk->StoredReplicas()) {
                        TChunkPtrWithIndexes chunkWithIndexes(chunk, replica.GetReplicaIndex(), replica.GetMediumIndex());

                        // If chunk is lost on some media, don't match dst medium with
                        // src medium: we want to be able to do cross-medium replication.
                        bool mediumMatches =
                            Any(allMediaStatistics.Status & ECrossMediumChunkStatus::MediumWiseLost) ||
                            mediumIndex == replica.GetMediumIndex();

                        if (mediumMatches &&
                            (chunk->IsRegular() ||
                             chunk->IsErasure() && replica.GetReplicaIndex() == replicaIndex ||
                             chunk->IsJournal() && replica.GetReplicaIndex() == SealedChunkReplicaIndex))
                        {
                            auto* node = replica.GetPtr();
                            if (node->GetLocalState() == ENodeState::Online) {
                                node->AddToChunkReplicationQueue(chunkWithIndexes, mediumIndex, priority);
                            }
                        }
                    }
                }
            }

            if (Any(statistics.Status & EChunkStatus::Sealed)) {
                YT_ASSERT(chunk->IsJournal());
                for (auto replica : chunk->StoredReplicas()) {
                    if (replica.GetMediumIndex() == mediumIndex &&
                        replica.GetReplicaIndex() == UnsealedChunkReplicaIndex)
                    {
                        auto* node = replica.GetPtr();
                        if (node->GetLocalState() == ENodeState::Online) {
                            TChunkPtrWithIndexes chunkWithIndexes(chunk, GenericChunkReplicaIndex, mediumIndex);
                            node->AddToChunkSealQueue(chunkWithIndexes);
                        }
                    }
                }
            }

            if (None(statistics.Status & EChunkStatus::Lost)) {
                if (Any(statistics.Status & (EChunkStatus::DataMissing | EChunkStatus::ParityMissing))) {
                    TChunkPtrWithIndexes chunkWithIndexes(chunk, GenericChunkReplicaIndex, mediumIndex);
                    AddToChunkRepairQueue(chunkWithIndexes, EChunkRepairQueue::Missing);
                } else if (Any(statistics.Status & (EChunkStatus::DataDecommissioned | EChunkStatus::ParityDecommissioned))) {
                    TChunkPtrWithIndexes chunkWithIndexes(chunk, GenericChunkReplicaIndex, mediumIndex);
                    AddToChunkRepairQueue(chunkWithIndexes, EChunkRepairQueue::Decommissioned);
                }
            }
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

    if (chunk->IsErasure()) {
        DataMissingChunks_.erase(chunk);
        ParityMissingChunks_.erase(chunk);
    }

    if (chunk->IsJournal()) {
        QuorumMissingChunks_.erase(chunk);
    }
}

void TChunkReplicator::RemoveChunkFromQueuesOnRefresh(TChunk* chunk)
{
    for (auto replica : chunk->StoredReplicas()) {
        auto* node = replica.GetPtr();

        // Remove from replication queue.
        TChunkPtrWithIndexes chunkWithIndexes(chunk, replica.GetReplicaIndex(), replica.GetMediumIndex());
        node->RemoveFromChunkReplicationQueues(chunkWithIndexes, AllMediaIndex);

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
        node->RemoveFromChunkReplicationQueues(chunkWithIndexes, AllMediaIndex);
        node->RemoveFromChunkSealQueue(chunkWithIndexes);
    }

    // Remove chunk from repair queues.
    if (chunk->IsErasure()) {
        const auto& requisition = chunk->GetAggregatedRequisition(GetChunkRequisitionRegistry());
        for (const auto& entry : requisition) {
            const auto& mediumIndex = entry.MediumIndex;
            TChunkPtrWithIndexes chunkPtrWithIndexes(chunk, GenericChunkReplicaIndex, mediumIndex);
            RemoveFromChunkRepairQueues(chunkPtrWithIndexes);
        }
    }
}

void TChunkReplicator::CancelChunkJobs(TChunk* chunk)
{
    auto job = chunk->GetJob();
    if (job) {
        YT_LOG_DEBUG("Job canceled (JobId: %v)", job->GetJobId());
        UnregisterJob(job);
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
    if (!IsObjectAlive(chunk)) {
        return;
    }

    if (chunk->IsForeign()) {
        return;
    }

    RefreshScanner_->EnqueueChunk(chunk);
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

void TChunkReplicator::ScheduleGlobalChunkRefresh(TChunk* frontChunk, int chunkCount)
{
    RefreshScanner_->ScheduleGlobalScan(frontChunk, chunkCount);
}

void TChunkReplicator::OnRefresh()
{
    if (!GetDynamicConfig()->EnableChunkRefresh) {
        YT_LOG_DEBUG("Chunk refresh disabled; see //sys/@config");
        return;
    }

    int totalCount = 0;
    int aliveCount = 0;
    NProfiling::TWallTimer timer;

    YT_LOG_DEBUG("Chunk refresh iteration started");

    auto deadline = GetCpuInstant() - DurationToCpuDuration(GetDynamicConfig()->ChunkRefreshDelay);
    PROFILE_AGGREGATED_TIMING (RefreshTimeCounter) {
        while (totalCount < GetDynamicConfig()->MaxChunksPerRefresh &&
               RefreshScanner_->HasUnscannedChunk(deadline))
        {
            if (timer.GetElapsedTime() > GetDynamicConfig()->MaxTimePerRefresh) {
                break;
            }

            ++totalCount;
            auto* chunk = RefreshScanner_->DequeueChunk();
            if (!chunk) {
                continue;
            }

            RefreshChunk(chunk);
            ++aliveCount;
        }
    }

    YT_LOG_DEBUG("Chunk refresh iteration completed (TotalCount: %v, AliveCount: %v)",
        totalCount,
        aliveCount);
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
            YT_LOG_INFO("Chunk replicator is disabled, see //sys/@config");
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
    auto statistics = multicellManager->ComputeClusterStatistics();
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

int TChunkReplicator::GetRefreshQueueSize() const
{
    return RefreshScanner_->GetQueueSize();
}

int TChunkReplicator::GetRequisitionUpdateQueueSize() const
{
    return RequisitionUpdateScanner_->GetQueueSize();
}

void TChunkReplicator::ScheduleRequisitionUpdate(TChunkList* chunkList)
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
            YT_VERIFY(IsObjectAlive(Root_));
            auto callbacks = CreatePreemptableChunkTraverserCallbacks(
                Bootstrap_,
                NCellMaster::EAutomatonThreadQueue::ChunkRequisitionUpdateTraverser);
            TraverseChunkTree(std::move(callbacks), this, Root_);
        }

    private:
        TBootstrap* const Bootstrap_;
        const TChunkReplicatorPtr Owner_;
        TChunkList* const Root_;

        virtual bool OnChunk(
            TChunk* chunk,
            i64 /*rowIndex*/,
            std::optional<i32> /*tabletIndex*/,
            const TReadLimit& /*startLimit*/,
            const TReadLimit& /*endLimit*/,
            TTransactionId /*timestampTransactionId*/) override
        {
            Owner_->ScheduleRequisitionUpdate(chunk);
            return true;
        }

        virtual bool OnChunkView(TChunkView* /*chunkView*/) override
        {
            return false;
        }

        virtual bool OnDynamicStore(
            TDynamicStore* /*dynamicStore*/,
            const NChunkClient::TReadLimit& /*startLimit*/,
            const NChunkClient::TReadLimit& /*endLimit*/) override
        {
            return true;
        }

        virtual void OnFinish(const TError& error) override
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

    RequisitionUpdateScanner_->EnqueueChunk(chunk);
}

void TChunkReplicator::OnRequisitionUpdate()
{
    if (!Bootstrap_->GetHydraFacade()->GetHydraManager()->IsActiveLeader()) {
        return;
    }

    if (!GetDynamicConfig()->EnableChunkRequisitionUpdate) {
        YT_LOG_DEBUG("Chunk requisition update disabled; see //sys/@config");
        return;
    }

    TReqUpdateChunkRequisition request;

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    request.set_cell_tag(multicellManager->GetCellTag());

    int totalCount = 0;
    int aliveCount = 0;
    NProfiling::TWallTimer timer;

    YT_LOG_DEBUG("Chunk requisition update iteration started");

    TmpRequisitionRegistry_.Clear();
    PROFILE_AGGREGATED_TIMING (RequisitionUpdateTimeCounter) {
        ClearChunkRequisitionCache();
        while (totalCount < GetDynamicConfig()->MaxChunksPerRequisitionUpdate &&
               RequisitionUpdateScanner_->HasUnscannedChunk())
        {
            if (timer.GetElapsedTime() > GetDynamicConfig()->MaxTimePerRequisitionUpdate) {
                break;
            }

            ++totalCount;
            auto* chunk = RequisitionUpdateScanner_->DequeueChunk();
            if (!chunk) {
                continue;
            }

            ComputeChunkRequisitionUpdate(chunk, &request);
            ++aliveCount;
        }
    }

    FillChunkRequisitionDict(&request, TmpRequisitionRegistry_);

    YT_LOG_DEBUG("Chunk requisition update iteration completed (TotalCount: %v, AliveCount: %v, UpdateCount: %v)",
        totalCount,
        aliveCount,
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
    SmallVector<TChunkList*, 64> queue;
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

void TChunkReplicator::RegisterJob(const TJobPtr& job)
{
    job->GetNode()->RegisterJob(job);

    auto jobType = job->GetType();
    ++RunningJobs_[jobType];
    ++JobsStarted_[jobType];

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto chunkId = job->GetChunkIdWithIndexes().Id;
    auto* chunk = chunkManager->FindChunk(chunkId);
    if (chunk) {
        chunk->SetJob(job);
    }

    UpdateInterDCEdgeConsumption(job, job->GetNode()->GetDataCenter(), +1);
}

void TChunkReplicator::UnregisterJob(const TJobPtr& job)
{
    job->GetNode()->UnregisterJob(job);
    auto jobType = job->GetType();
    --RunningJobs_[jobType];

    auto jobState = job->GetState();
    switch (jobState) {
        case EJobState::Completed:
            ++JobsCompleted_[jobType];
            break;
        case EJobState::Failed:
            ++JobsFailed_[jobType];
            break;
        case EJobState::Aborted:
            ++JobsAborted_[jobType];
            break;
        default:
            break;
    }
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto chunkId = job->GetChunkIdWithIndexes().Id;
    auto* chunk = chunkManager->FindChunk(chunkId);
    if (chunk) {
        chunk->SetJob(nullptr);
        ScheduleChunkRefresh(chunk);
    }

    UpdateInterDCEdgeConsumption(job, job->GetNode()->GetDataCenter(), -1);
}

void TChunkReplicator::OnNodeDataCenterChanged(TNode* node, TDataCenter* oldDataCenter)
{
    YT_ASSERT(node->GetDataCenter() != oldDataCenter);

    for (const auto& pair : node->IdToJob()) {
        const auto& job = pair.second;
        UpdateInterDCEdgeConsumption(job, oldDataCenter, -1);
        UpdateInterDCEdgeConsumption(job, node->GetDataCenter(), +1);
    }
}

void TChunkReplicator::AddToChunkRepairQueue(TChunkPtrWithIndexes chunkWithIndexes, EChunkRepairQueue queue)
{
    YT_ASSERT(chunkWithIndexes.GetReplicaIndex() == GenericChunkReplicaIndex);
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

void TChunkReplicator::InitInterDCEdges()
{
    UpdateInterDCEdgeCapacities();
    InitUnsaturatedInterDCEdges();
}

void TChunkReplicator::UpdateInterDCEdgeCapacities(bool force)
{
    if (!force &&
        GetCpuInstant() - InterDCEdgeCapacitiesLastUpdateTime_ <= GetDynamicConfig()->InterDCLimits->GetUpdateInterval())
    {
        return;
    }

    InterDCEdgeCapacities_.clear();

    auto capacities = GetDynamicConfig()->InterDCLimits->GetCapacities();

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();

    auto updateForSrcDC = [&] (const TDataCenter* srcDataCenter) {
        const std::optional<TString>& srcDataCenterName = srcDataCenter
            ? std::optional<TString>(srcDataCenter->GetName())
            : std::nullopt;
        auto& interDCEdgeCapacities = InterDCEdgeCapacities_[srcDataCenter];
        const auto& newInterDCEdgeCapacities = capacities[srcDataCenterName];

        auto updateForDstDC = [&] (const TDataCenter* dstDataCenter) {
            const std::optional<TString>& dstDataCenterName = dstDataCenter
                ? std::optional<TString>(dstDataCenter->GetName())
                : std::nullopt;
            auto it = newInterDCEdgeCapacities.find(dstDataCenterName);
            if (it != newInterDCEdgeCapacities.end()) {
                interDCEdgeCapacities[dstDataCenter] = it->second / GetCappedSecondaryCellCount();
            }
        };

        updateForDstDC(nullptr);
        for (const auto& pair : nodeTracker->DataCenters()) {
            if (IsObjectAlive(pair.second)) {
                updateForDstDC(pair.second);
            }
        }
    };

    updateForSrcDC(nullptr);
    for (const auto& pair : nodeTracker->DataCenters()) {
        if (IsObjectAlive(pair.second)) {
            updateForSrcDC(pair.second);
        }
    }

    InterDCEdgeCapacitiesLastUpdateTime_ = GetCpuInstant();
}

void TChunkReplicator::InitUnsaturatedInterDCEdges()
{
    UnsaturatedInterDCEdges_.clear();

    const auto defaultCapacity = GetDynamicConfig()->InterDCLimits->GetDefaultCapacity() / GetCappedSecondaryCellCount();

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();

    auto updateForSrcDC = [&] (const TDataCenter* srcDataCenter) {
        auto& interDCEdgeConsumption = InterDCEdgeConsumption_[srcDataCenter];
        const auto& interDCEdgeCapacities = InterDCEdgeCapacities_[srcDataCenter];

        auto updateForDstDC = [&] (const TDataCenter* dstDataCenter) {
            if (interDCEdgeConsumption.Value(dstDataCenter, 0) <
                interDCEdgeCapacities.Value(dstDataCenter, defaultCapacity))
            {
                UnsaturatedInterDCEdges_[srcDataCenter].insert(dstDataCenter);
            }
        };

        updateForDstDC(nullptr);
        for (const auto& pair : nodeTracker->DataCenters()) {
            if (IsObjectAlive(pair.second)) {
                updateForDstDC(pair.second);
            }
        }
    };

    updateForSrcDC(nullptr);
    for (const auto& pair : nodeTracker->DataCenters()) {
        if (IsObjectAlive(pair.second)) {
            updateForSrcDC(pair.second);
        }
    }
}

void TChunkReplicator::UpdateInterDCEdgeConsumption(
    const TJobPtr& job,
    const TDataCenter* srcDataCenter,
    int sizeMultiplier)
{
    if (job->GetType() != EJobType::ReplicateChunk &&
        job->GetType() != EJobType::RepairChunk)
    {
        return;
    }

    auto& interDCEdgeConsumption = InterDCEdgeConsumption_[srcDataCenter];
    const auto& interDCEdgeCapacities = InterDCEdgeCapacities_[srcDataCenter];

    const auto defaultCapacity = GetDynamicConfig()->InterDCLimits->GetDefaultCapacity() / GetCappedSecondaryCellCount();

    for (const auto& nodePtrWithIndexes : job->TargetReplicas()) {
        const auto* dstDataCenter = nodePtrWithIndexes.GetPtr()->GetDataCenter();

        i64 chunkPartSize = 0;
        switch (job->GetType()) {
            case EJobType::ReplicateChunk:
                chunkPartSize = job->ResourceUsage().replication_data_size();
                break;
            case EJobType::RepairChunk:
                chunkPartSize = job->ResourceUsage().repair_data_size();
                break;
            default:
                YT_ABORT();
        }

        auto& consumption = interDCEdgeConsumption[dstDataCenter];
        consumption += sizeMultiplier * chunkPartSize;

        if (consumption < interDCEdgeCapacities.Value(dstDataCenter, defaultCapacity)) {
            UnsaturatedInterDCEdges_[srcDataCenter].insert(dstDataCenter);
        } else {
            auto it = UnsaturatedInterDCEdges_.find(srcDataCenter);
            if (it != UnsaturatedInterDCEdges_.end()) {
                it->second.erase(dstDataCenter);
                // Don't do UnsaturatedInterDCEdges_.erase(it) here - the memory
                // saving is negligible, but the slowdown may be noticeable. Plus,
                // the removal is very likely to be undone by a soon-to-follow insertion.
            }
        }
    }
}

bool TChunkReplicator::HasUnsaturatedInterDCEdgeStartingFrom(const TDataCenter* srcDataCenter)
{
    return !UnsaturatedInterDCEdges_[srcDataCenter].empty();
}

void TChunkReplicator::OnDataCenterCreated(const TDataCenter* dataCenter)
{
    UpdateInterDCEdgeCapacities(true);

    const auto defaultCapacity = GetDynamicConfig()->InterDCLimits->GetDefaultCapacity() / GetCappedSecondaryCellCount();

    auto updateEdge = [&] (const TDataCenter* srcDataCenter, const TDataCenter* dstDataCenter) {
        if (InterDCEdgeConsumption_[srcDataCenter].Value(dstDataCenter, 0) <
            InterDCEdgeCapacities_[srcDataCenter].Value(dstDataCenter, defaultCapacity))
        {
            UnsaturatedInterDCEdges_[srcDataCenter].insert(dstDataCenter);
        }
    };

    updateEdge(nullptr, dataCenter);
    updateEdge(dataCenter, nullptr);

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    for (const auto& [dstDataCenterId, otherDataCenter] : nodeTracker->DataCenters()) {
        updateEdge(dataCenter, otherDataCenter);
        updateEdge(otherDataCenter, dataCenter);
    }
}

void TChunkReplicator::OnDataCenterDestroyed(const TDataCenter* dataCenter)
{
    InterDCEdgeCapacities_.erase(dataCenter);
    for (auto& [srcDataCenter, dstDataCenterCapacities] : InterDCEdgeCapacities_) {
        dstDataCenterCapacities.erase(dataCenter); // may be no-op
    }

    InterDCEdgeConsumption_.erase(dataCenter);
    for (auto& [srcDataCenter, dstDataCenterConsumption] : InterDCEdgeConsumption_) {
        dstDataCenterConsumption.erase(dataCenter); // may be no-op
    }

    UnsaturatedInterDCEdges_.erase(dataCenter);
    for (auto& [srcDataCenter, dstDataCenterSet] : UnsaturatedInterDCEdges_) {
        dstDataCenterSet.erase(dataCenter); // may be no-op
    }
}

TChunkRequisitionRegistry* TChunkReplicator::GetChunkRequisitionRegistry()
{
    return Bootstrap_->GetChunkManager()->GetChunkRequisitionRegistry();
}

int TChunkReplicator::GetCappedSecondaryCellCount()
{
    return std::max<int>(1, Bootstrap_->GetMulticellManager()->GetSecondaryCellTags().size());
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

const TDynamicChunkManagerConfigPtr& TChunkReplicator::GetDynamicConfig()
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    return configManager->GetConfig()->ChunkManager;
}

void TChunkReplicator::OnDynamicConfigChanged()
{
    RefreshExecutor_->SetPeriod(GetDynamicConfig()->ChunkRefreshPeriod);
    RequisitionUpdateExecutor_->SetPeriod(GetDynamicConfig()->ChunkRequisitionUpdatePeriod);
    FinishedRequisitionTraverseFlushExecutor_->SetPeriod(GetDynamicConfig()->FinishedChunkListsRequisitionTraverseFlushPeriod);
    JobThrottler_->Reconfigure(GetDynamicConfig()->JobThrottler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
