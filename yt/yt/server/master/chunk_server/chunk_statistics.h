#pragma once

#include "chunk_replica.h"
#include "chunk_requisition.h"
#include "stored_chunk_replica.h"

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/library/erasure/impl/public.h>

#include <array>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TPerMediumChunkStatistics
{
    EChunkStatus Status = EChunkStatus::None;

    //! Number of active replicas, per each replica index.
    std::array<int, ChunkReplicaIndexBound> ReplicaCount{};

    //! Number of decommissioned replicas, per each replica index.
    std::array<int, ChunkReplicaIndexBound> DecommissionedReplicaCount{};

    //! Number of replicas on pending restart nodes, per each replica index.
    std::array<int, ChunkReplicaIndexBound> TemporarilyUnavailableReplicaCount{};

    //! Indexes of replicas whose replication is advised.
    TCompactVector<int, TypicalReplicaCount> ReplicationIndexes;

    //! Decommissioned replicas whose removal is advised.
    // NB: There's no actual need to have medium index in context of this
    // per-medium class. This is just for convenience.
    TChunkLocationPtrWithReplicaIndexList DecommissionedRemovalReplicas;

    //! Indexes of replicas whose removal is advised for balancing.
    TCompactVector<int, TypicalReplicaCount> BalancingRemovalIndexes;

    //! Any replica that violates failure domain placement.
    TAugmentedStoredChunkReplicaPtr UnsafelyPlacedReplica;

    //! Missing chunk replicas for CRP-enabled chunks.
    TNodePtrWithReplicaAndMediumIndexList MissingReplicas;
};

struct TChunkStatistics
{
    TCompactMediumMap<TPerMediumChunkStatistics> PerMediumStatistics;
    ECrossMediumChunkStatus Status = ECrossMediumChunkStatus::None;
};

////////////////////////////////////////////////////////////////////////////////

struct IChunkStatisticsCalculatorCallbacks
    : public virtual TRefCounted
{
    virtual TMedium* FindMediumByIndex(int mediumIndex) const = 0;
    virtual const NHydra::TReadOnlyEntityMap<TMedium>& GetMedia() const = 0;
    virtual TChunkRequisitionRegistry* GetChunkRequisitionRegistry() const = 0;
    virtual const TDynamicChunkManagerConfigPtr& GetDynamicConfig() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkStatisticsCalculatorCallbacks)

////////////////////////////////////////////////////////////////////////////////

class TChunkStatisticsCalculator
{
public:
    TChunkStatisticsCalculator(
        TChunkManagerConfigPtr config,
        TChunkPlacementPtr chunkPlacement,
        IChunkStatisticsCalculatorCallbacksPtr callbacks);

    TChunkStatistics ComputeChunkStatistics(
        const TChunk* chunk,
        const TStoredChunkReplicaList& replicas);

    //! Same as corresponding #TChunk method but
    //!   - replication factors are capped by medium-specific bounds;
    //!   - additional entries may be introduced if the chunk has replicas
    //!     stored on a medium it's not supposed to have replicas on.
    TChunkReplication GetChunkAggregatedReplication(
        const TChunk* chunk,
        const TStoredChunkReplicaList& replicas) const;

    //! Same as corresponding #TChunk method but the result is capped by the medium-specific bound.
    int GetChunkAggregatedReplicationFactor(const TChunk* chunk, int mediumIndex) const;

private:
    const TChunkManagerConfigPtr Config_;
    const TChunkPlacementPtr ChunkPlacement_;
    const IChunkStatisticsCalculatorCallbacksPtr Callbacks_;

    TChunkStatistics ComputeRegularChunkStatistics(
        const TChunk* chunk,
        const TStoredChunkReplicaList& replicas);
    void ComputeRegularChunkStatisticsForMedium(
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
        const TNodePtrWithReplicaAndMediumIndexList& missingReplicas);

    void ComputeRegularChunkStatisticsCrossMedia(
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
        bool hasMediumOnWhichSealedMissing);

    TChunkStatistics ComputeErasureChunkStatistics(
        const TChunk* chunk,
        const TStoredChunkReplicaList& replicas);
    void ComputeErasureChunkStatisticsForMedium(
        TPerMediumChunkStatistics& result,
        NErasure::ICodec* codec,
        TReplicationPolicy replicationPolicy,
        int maxReplicasPerRack,
        const std::array<TChunkLocationList, ChunkReplicaIndexBound>& decommissionedReplicas,
        TAugmentedStoredChunkReplicaPtr unsafelyPlacedSealedReplica,
        NErasure::TPartIndexSet& erasedIndexes,
        bool totallySealed);
    void ComputeErasureChunkStatisticsCrossMedia(
        TChunkStatistics& result,
        const TChunk* chunk,
        NErasure::ICodec* codec,
        bool allMediaTransient,
        bool allMediaDataPartsOnly,
        const TCompactMediumMap<NErasure::TPartIndexSet>& mediumToErasedIndexes,
        const TMediumSet& activeMedia,
        const NErasure::TPartIndexSet& replicaIndexes,
        bool totallySealed);

    TCompactMediumMap<TNodeList> GetChunkConsistentPlacementNodes(
        const TChunk* chunk,
        const TStoredChunkReplicaList& replicas);

    bool IsConsistentChunkPlacementEnabled() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
