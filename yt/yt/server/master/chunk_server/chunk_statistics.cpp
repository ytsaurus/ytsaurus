#include "chunk_statistics.h"

#include "chunk.h"
#include "chunk_location.h"
#include "chunk_placement.h"
#include "config.h"
#include "domestic_medium.h"
#include "helpers.h"
#include "medium_base.h"
#include "private.h"

#include <yt/yt/server/master/node_tracker_server/data_center.h>
#include <yt/yt/server/master/node_tracker_server/host.h>
#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/rack.h>

#include <yt/yt/library/erasure/impl/codec.h>

namespace NYT::NChunkServer {

using namespace NChunkClient;
using namespace NNodeTrackerServer;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ChunkServerLogger;

constexpr int MinAvailableReplicaCount = 1;
constexpr int MaxTemporarilyUnavailableReplicaCount = 1;

////////////////////////////////////////////////////////////////////////////////

TChunkStatisticsCalculator::TChunkStatisticsCalculator(
    TChunkManagerConfigPtr config,
    TChunkPlacementPtr chunkPlacement,
    IChunkStatisticsCalculatorCallbacksPtr callbacks)
    : Config_(std::move(config))
    , ChunkPlacement_(std::move(chunkPlacement))
    , Callbacks_(std::move(callbacks))
{ }

////////////////////////////////////////////////////////////////////////////////

TChunkStatistics TChunkStatisticsCalculator::ComputeChunkStatistics(
    const TChunk* chunk,
    const TStoredChunkReplicaList& replicas)
{
    if (chunk->IsErasure()) {
        TStoredChunkReplicaList offshoreReplicas;
        for (const auto& replica : replicas) {
            if (replica.GetStoredReplicaType() == EStoredReplicaType::OffshoreMedia) {
                offshoreReplicas.push_back(replica);
            }
        }
        YT_LOG_ALERT_UNLESS(
            offshoreReplicas.empty(),
            "Erasure chunk has offshore replicas (ChunkId: %v, Replicas: %v, OffshoreReplicas: %v)",
            chunk->GetId(),
            replicas,
            offshoreReplicas);
    }

    auto result = chunk->IsErasure()
        ? ComputeErasureChunkStatistics(chunk, replicas)
        : ComputeRegularChunkStatistics(chunk, replicas);

    if (chunk->IsJournal() && chunk->IsSealed()) {
        result.Status |= ECrossMediumChunkStatus::Sealed;
    }

    return result;
}

TChunkStatistics TChunkStatisticsCalculator::ComputeErasureChunkStatistics(
    const TChunk* chunk,
    const TStoredChunkReplicaList& replicas)
{
    TChunkStatistics result;

    auto* codec = NErasure::GetCodec(chunk->GetErasureCodec());

    TCompactMediumMap<std::array<TChunkLocationList, ChunkReplicaIndexBound>> decommissionedReplicas;
    TCompactMediumMap<std::array<ui8, RackIndexBound>> perRackReplicaCounters;
    TCompactMediumMap<THashSet<const THost*>> replicasHosts;
    // TODO(gritukan): YT-16557.
    TCompactMediumMap<THashMap<const TDataCenter*, ui8>> perDataCenterReplicaCounters;

    // An arbitrary replica collocated with too may others within a single rack - per medium.
    TCompactMediumMap<TAugmentedStoredChunkReplicaPtr> unsafelyPlacedSealedReplicas;
    // An arbitrary replica that violates consistent placement requirements - per medium.
    TCompactMediumMap<std::array<TChunkLocation*, ChunkReplicaIndexBound>> inconsistentlyPlacedSealedReplicas;

    TCompactMediumMap<int> replicaCount;
    TCompactMediumMap<int> decommissionedReplicaCount;
    TCompactMediumMap<int> temporarilyUnavailableReplicaCount;

    NErasure::TPartIndexSet replicaIndexes;

    auto totallySealed = chunk->IsSealed();

    auto mark = TNode::GenerateVisitMark();
    auto chunkReplication = GetChunkAggregatedReplication(chunk, replicas);
    for (const auto& entry : chunkReplication) {
        auto mediumIndex = entry.GetMediumIndex();
        unsafelyPlacedSealedReplicas[mediumIndex] = {};
        replicaCount[mediumIndex] = 0;
        decommissionedReplicaCount[mediumIndex] = 0;
    }

    for (auto replica : replicas) {
        auto* locationReplica = replica.As<EStoredReplicaType::ChunkLocation>();
        if (!locationReplica) {
            YT_LOG_ALERT("Non-chunk location stored replica encountered during computation statistics for erasure chunk "
                "(ChunkId: %v, ReplicaMediumIndex: %v, ReplicaIndex: %v)",
                chunk->GetId(),
                replica.GetEffectiveMediumIndex(),
                replica.GetReplicaIndex());
            continue;
        }
        auto* chunkLocation = locationReplica->AsChunkLocationPtr();
        auto node = chunkLocation->GetNode();
        int replicaIndex = replica.GetReplicaIndex();
        int mediumIndex = chunkLocation->GetEffectiveMediumIndex();
        auto& mediumStatistics = result.PerMediumStatistics[mediumIndex];

        replicaIndexes.set(replicaIndex);

        auto isReplicaSealed = !chunk->IsJournal() || replica.GetReplicaState() == EChunkReplicaState::Sealed;

        if (!isReplicaSealed) {
            totallySealed = false;
        }

        if (IsReplicaDecommissioned(chunkLocation) || node->GetVisitMark(mediumIndex) == mark) {
            ++mediumStatistics.DecommissionedReplicaCount[replicaIndex];
            decommissionedReplicas[mediumIndex][replicaIndex].push_back(chunkLocation);
            ++decommissionedReplicaCount[mediumIndex];
        } else if (IsReplicaOnPendingRestartNode(chunkLocation)) {
            ++mediumStatistics.TemporarilyUnavailableReplicaCount[replicaIndex];
            ++temporarilyUnavailableReplicaCount[mediumIndex];
        } else {
            ++mediumStatistics.ReplicaCount[replicaIndex];
            ++replicaCount[mediumIndex];
        }

        if (!Config_->AllowMultipleErasurePartsPerNode) {
            node->SetVisitMark(mediumIndex, mark);
        }

        auto host = chunkLocation->GetNode()->GetHost();
        if (ChunkPlacement_->UseHostAwareReplicator() && host) {
            auto [it, inserted] = replicasHosts[mediumIndex].insert(host);
            if (!inserted) {
                YT_LOG_TRACE(
                    "Chunk has multiple replicas on the same host (ChunkId: %v, Host: %v, UnsafelyPlacedReplicaNodeAddress: %v)",
                    chunk->GetId(),
                    host->GetName(),
                    chunkLocation->GetNode()->GetDefaultAddress());
                unsafelyPlacedSealedReplicas[mediumIndex] = replica;
            }
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

            if (auto dataCenter = rack->GetDataCenter()) {
                auto maxReplicasPerDataCenter = ChunkPlacement_->GetMaxReplicasPerDataCenter(mediumIndex, chunk, dataCenter);
                auto replicasPerDataCenter = ++perDataCenterReplicaCounters[mediumIndex][dataCenter];
                if (replicasPerDataCenter > maxReplicasPerDataCenter && isReplicaSealed) {
                    unsafelyPlacedSealedReplicas[mediumIndex] = replica;
                }
            }
        }
    }

    auto allMediaTransient = true;
    auto allMediaDataPartsOnly = true;
    TCompactMediumMap<NErasure::TPartIndexSet> mediumToErasedIndexes;
    TMediumSet activeMedia;

    for (const auto& entry : chunkReplication) {
        auto mediumIndex = entry.GetMediumIndex();
        auto* mediumBase = Callbacks_->FindMediumByIndex(mediumIndex);
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

        auto maxReplicasPerRack = ChunkPlacement_->GetMaxReplicasPerRack(mediumIndex, chunk);

        auto& mediumStatistics = result.PerMediumStatistics[mediumIndex];
        ComputeErasureChunkStatisticsForMedium(
            mediumStatistics,
            codec,
            replicationPolicy,
            maxReplicasPerRack,
            decommissionedReplicas[mediumIndex],
            unsafelyPlacedSealedReplicas[mediumIndex],
            mediumToErasedIndexes[mediumIndex],
            totallySealed);

        YT_LOG_TRACE("Computed erasure chunk statistics for medium "
            "(ChunkId: %v, MediumIndex: %v, MediumName: %v, DataPartsOnly: %v, Status: %v, "
            "ReplicationFactor: %v, ReplicaCount: %v, MaxReplicasPerRack: %v, "
            "DecommissionedReplicaCount: %v, TemporarilyUnavailableReplicaCount: %v)",
            chunk->GetId(),
            mediumIndex,
            medium->GetName(),
            dataPartsOnly,
            mediumStatistics.Status,
            mediumReplicationFactor,
            replicaCount[mediumIndex],
            maxReplicasPerRack,
            decommissionedReplicaCount[mediumIndex],
            temporarilyUnavailableReplicaCount[mediumIndex]);
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

TCompactMediumMap<TNodeList> TChunkStatisticsCalculator::GetChunkConsistentPlacementNodes(
    const TChunk* chunk,
    const TStoredChunkReplicaList& replicas)
{
    if (!chunk->HasConsistentReplicaPlacementHash()) {
        return {};
    }

    if (!IsConsistentChunkPlacementEnabled()) {
        return {};
    }

    TCompactMediumMap<TNodeList> result;
    auto chunkReplication = GetChunkAggregatedReplication(chunk, replicas);
    for (const auto& entry : chunkReplication) {
        auto mediumPolicy = entry.Policy();
        if (!mediumPolicy) {
            continue;
        }

        auto mediumIndex = entry.GetMediumIndex();
        auto* medium = Callbacks_->FindMediumByIndex(mediumIndex);
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
            chunk->GetPhysicalReplicationFactor(
                mediumIndex,
                Callbacks_->GetChunkRequisitionRegistry()));
    }

    return result;
}

void TChunkStatisticsCalculator::ComputeErasureChunkStatisticsForMedium(
    TPerMediumChunkStatistics& result,
    NErasure::ICodec* codec,
    TReplicationPolicy replicationPolicy,
    int maxReplicasPerRack,
    const std::array<TChunkLocationList, ChunkReplicaIndexBound>& decommissionedReplicas,
    TAugmentedStoredChunkReplicaPtr unsafelyPlacedSealedReplica,
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
        temporarilyUnavailablePartCount + maxReplicasPerRack <=
        codec->GetGuaranteedRepairablePartCount();
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

void TChunkStatisticsCalculator::ComputeErasureChunkStatisticsCrossMedia(
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
        for (const auto& mediumIdAndPtrPair : Callbacks_->GetMedia()) {
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

    auto totalPartCount = codec->GetTotalPartCount();
    auto dataPartCount = codec->GetDataPartCount();

    auto crossMediaDataMissing = false;
    auto crossMediaParityMissing = false;
    auto precarious = false;
    auto crossMediaLost = false;

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

TChunkStatistics TChunkStatisticsCalculator::ComputeRegularChunkStatistics(
    const TChunk* chunk,
    const TStoredChunkReplicaList& replicas)
{
    TChunkStatistics results;

    TMediumMap<TAugmentedStoredChunkReplicaPtr> unsafelyPlacedReplicas;
    TMediumMap<std::array<ui8, RackIndexBound>> perRackReplicaCounters;
    TMediumMap<THashSet<const THost*>> replicasHosts;
    // TODO(gritukan): YT-16557.
    TMediumMap<THashMap<const TDataCenter*, ui8>> perDataCenterReplicaCounters;

    // An arbitrary replica that violates consistent placement requirements - per medium.
    TMediumMap<TChunkLocationPtrWithReplicaIndex> inconsistentlyPlacedReplica;

    TMediumMap<int> replicaCount;
    TMediumMap<int> decommissionedReplicaCount;
    TMediumMap<int> temporarilyUnavailableReplicaCount;
    TMediumMap<TChunkLocationPtrWithReplicaIndexList> decommissionedReplicas;
    TMediumMap<TChunkLocationPtrWithReplicaIndexList> temporarilyUnavailableReplicas;
    int totalReplicaCount = 0;
    int totalDecommissionedReplicaCount = 0;

    TMediumMap<TNodePtrWithReplicaAndMediumIndexList> missingReplicas;

    TMediumSet hasSealedReplica;
    auto hasSealedReplicas = false;
    auto totallySealed = chunk->IsSealed();

    auto consistentPlacementNodes = GetChunkConsistentPlacementNodes(chunk, replicas);

    for (const auto& [mediumIndex, consistentNodes] : consistentPlacementNodes) {
        for (auto node : consistentNodes) {
            TNodePtrWithReplicaAndMediumIndex nodePtrWithIndexes(node, GenericChunkReplicaIndex, mediumIndex);
            auto it = std::find_if(
                replicas.begin(),
                replicas.end(),
                [&, mediumIndex = mediumIndex] (const TAugmentedStoredChunkReplicaPtr& replica) {
                    auto* locationReplica = replica.As<EStoredReplicaType::ChunkLocation>();
                    if (!locationReplica) {
                        return false;
                    }
                    return locationReplica->AsChunkLocationPtr()->GetNode() == node && replica.GetEffectiveMediumIndex() == mediumIndex;
                });
            if (it == replicas.end()) {
                missingReplicas[mediumIndex].push_back(nodePtrWithIndexes);
            }
        }
    }

    for (auto replica : replicas) {
        auto* locationReplica = replica.As<EStoredReplicaType::ChunkLocation>();
        if (!locationReplica) {
            // TODO(cherepashka): support offshore media.
            continue;
        }
        auto* chunkLocation = locationReplica->AsChunkLocationPtr();
        auto node = chunkLocation->GetNode();
        auto mediumIndex = chunkLocation->GetEffectiveMediumIndex();

        if (chunk->IsJournal() && replica.GetReplicaState() != EChunkReplicaState::Sealed) {
            totallySealed = false;
        } else {
            hasSealedReplica[mediumIndex] = true;
            hasSealedReplicas = true;
        }

        if (IsReplicaDecommissioned(chunkLocation)) {
            ++decommissionedReplicaCount[mediumIndex];
            decommissionedReplicas[mediumIndex].emplace_back(chunkLocation, replica.GetReplicaIndex());
            ++totalDecommissionedReplicaCount;
        } else if (IsReplicaOnPendingRestartNode(chunkLocation)) {
            ++temporarilyUnavailableReplicaCount[mediumIndex];
            temporarilyUnavailableReplicas[mediumIndex].emplace_back(chunkLocation, replica.GetReplicaIndex());
        } else {
            ++replicaCount[mediumIndex];
            ++totalReplicaCount;
        }

        auto host = chunkLocation->GetNode()->GetHost();
        if (ChunkPlacement_->UseHostAwareReplicator() && host) {
            auto [it, inserted] = replicasHosts[mediumIndex].insert(host);
            if (!inserted) {
                YT_LOG_TRACE(
                    "Chunk has multiple replicas on the same host (ChunkId: %v, Host: %v, UnsafelyPlacedReplicaNodeAddress: %v)",
                    chunk->GetId(),
                    host->GetName(),
                    chunkLocation->GetNode()->GetDefaultAddress());
                unsafelyPlacedReplicas[mediumIndex] = replica;
            }
        }

        if (const auto* rack = chunkLocation->GetNode()->GetRack()) {
            int rackIndex = rack->GetIndex();
            auto maxReplicasPerRack = ChunkPlacement_->GetMaxReplicasPerRack(mediumIndex, chunk);
            if (++perRackReplicaCounters[mediumIndex][rackIndex] > maxReplicasPerRack) {
                unsafelyPlacedReplicas[mediumIndex] = replica;
            }

            if (auto dataCenter = rack->GetDataCenter()) {
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

    auto precarious = true;
    auto allMediaTransient = true;
    TCompactVector<int, MaxMediumCount> mediaOnWhichLost;
    auto hasMediumOnWhichPresent = false;
    auto hasMediumOnWhichUnderreplicated = false;
    auto hasMediumOnWhichSealedMissing = false;

    auto replication = GetChunkAggregatedReplication(chunk, replicas);
    for (auto entry : replication) {
        auto mediumIndex = entry.GetMediumIndex();
        auto* medium = Callbacks_->FindMediumByIndex(mediumIndex);
        YT_VERIFY(IsObjectAlive(medium));

        // TODO(gritukan): Check replica presence here when
        // chunk will store offshore replicas.
        // For now, we just ignore such media.
        if (medium->IsOffshore()) {
            continue;
        }

        auto& mediumStatistics = results.PerMediumStatistics[mediumIndex];
        auto mediumTransient = medium->IsDomestic() && medium->AsDomestic()->GetTransient();

        auto mediumReplicationPolicy = entry.Policy();
        auto mediumReplicaCount = replicaCount[mediumIndex];
        auto mediumDecommissionedReplicaCount = decommissionedReplicaCount[mediumIndex];
        auto mediumTemporarilyUnavailableReplicaCount = temporarilyUnavailableReplicaCount[mediumIndex];

        // NB: Some very counter-intuitive scenarios are possible here.
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

        auto maxReplicasPerRack = ChunkPlacement_->GetMaxReplicasPerRack(mediumIndex, chunk);

        ComputeRegularChunkStatisticsForMedium(
            mediumStatistics,
            chunk,
            mediumReplicationPolicy,
            maxReplicasPerRack,
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

        YT_LOG_TRACE("Computed regular chunk statistics for medium "
            "(ChunkId: %v, MediumIndex: %v, MediumName: %v, Status: %v, ReplicationFactor: %v, "
            "ReplicaCount: %v, MaxReplicasPerRack: %v, DecommissionedReplicas: %v, "
            "TemporarilyUnavailableReplicas: %v)",
            chunk->GetId(),
            mediumIndex,
            medium->GetName(),
            mediumStatistics.Status,
            mediumReplicationPolicy.GetReplicationFactor(),
            mediumReplicaCount,
            maxReplicasPerRack,
            MakeFormattableView(decommissionedReplicas[mediumIndex], TDefaultFormatter{}),
            MakeFormattableView(temporarilyUnavailableReplicas[mediumIndex], TDefaultFormatter{}));
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

void TChunkStatisticsCalculator::ComputeRegularChunkStatisticsForMedium(
    TPerMediumChunkStatistics& result,
    const TChunk* chunk,
    TReplicationPolicy replicationPolicy,
    int maxReplicasPerRack,
    int replicaCount,
    int decommissionedReplicaCount,
    int temporarilyUnavailableReplicaCount,
    const TChunkLocationPtrWithReplicaIndexList& decommissionedReplicas,
    bool hasSealedReplica,
    bool totallySealed,
    TAugmentedStoredChunkReplicaPtr unsafelyPlacedReplica,
    TChunkLocationPtrWithReplicaIndex inconsistentlyPlacedReplica,
    const TNodePtrWithReplicaAndMediumIndexList& missingReplicas)
{
    auto replicationFactor = replicationPolicy.GetReplicationFactor();
    auto minRackAwareReplicaCount = std::min(replicationFactor, MinAvailableReplicaCount + maxReplicasPerRack);
    auto minSafeAvailableReplicaCount = std::max(replicationFactor - MaxTemporarilyUnavailableReplicaCount, minRackAwareReplicaCount);
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
             replicaCount < minSafeAvailableReplicaCount) &&
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

    if (replicationFactor > 1 && unsafelyPlacedReplica && None(result.Status & EChunkStatus::Overreplicated)) {
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

void TChunkStatisticsCalculator::ComputeRegularChunkStatisticsCrossMedia(
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

bool TChunkStatisticsCalculator::IsConsistentChunkPlacementEnabled() const
{
    return Callbacks_->GetDynamicConfig()->ConsistentReplicaPlacement->Enable;
}

////////////////////////////////////////////////////////////////////////////////

TChunkReplication TChunkStatisticsCalculator::GetChunkAggregatedReplication(
    const TChunk* chunk,
    const TStoredChunkReplicaList& replicas) const
{
    auto result = chunk->GetAggregatedReplication(Callbacks_->GetChunkRequisitionRegistry());
    for (auto& entry : result) {
        YT_VERIFY(entry.Policy());

        auto* medium = Callbacks_->FindMediumByIndex(entry.GetMediumIndex());
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
        auto mediumIndex = replica.GetEffectiveMediumIndex();
        if (!result.Contains(mediumIndex)) {
            result.Set(mediumIndex, TReplicationPolicy(), /*eraseEmpty*/ false);
        }
    }

    return result;
}

int TChunkStatisticsCalculator::GetChunkAggregatedReplicationFactor(
    const TChunk* chunk,
    int mediumIndex) const
{
    auto result = chunk->GetAggregatedReplicationFactor(
        mediumIndex,
        Callbacks_->GetChunkRequisitionRegistry());

    auto* medium = Callbacks_->FindMediumByIndex(mediumIndex);
    YT_VERIFY(IsObjectAlive(medium));
    // For now, all offshore media have replication parameters in settings, so
    // from replicator's point of view chunk has single replica.
    auto cap = medium->IsDomestic()
        ? medium->AsDomestic()->Config()->MaxReplicationFactor
        : 1;

    return std::min(cap, result);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
