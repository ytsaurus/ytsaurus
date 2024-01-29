#pragma once

#include "public.h"

#include "chunk_requisition.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/core/misc/consistent_hashing_ring.h>

namespace NYT::NChunkServer {

///////////////////////////////////////////////////////////////////////////////

class TConsistentChunkPlacement
    : public TRefCounted
{
public:
    explicit TConsistentChunkPlacement(
        NCellMaster::TBootstrap* bootstrap,
        int chunkReplicaCount);

    // Marks the chunk on the ring (in its consistent locations).
    void AddChunk(TChunk* chunk) noexcept;
    void RemoveChunk(
        TChunk* chunk,
        const std::optional<TChunkReplication>& replication = {},
        bool missingOk = false) noexcept;

    // Returns chunk's consistent locations (without changing the ring).
    TNodeList GetWriteTargets(const TChunk* chunk, int mediumIndex) const;

    // Returns chunks to be refreshed.
    std::vector<TChunk*> AddNode(TNode* node);

    // Returns chunks to be refreshed.
    std::vector<TChunk*> RemoveNode(TNode* node);

    // Returns chunks to be refreshed.
    std::vector<TChunk*> UpdateNodeTokenCount(
        TNode* node,
        int mediumIndex,
        i64 oldTokenCount,
        i64 newTokenCount);

    // NB: Potentially very expensive. Removes all placement groups from all the
    // rings and then adds them back (with a new replica count).
    void SetChunkReplicaCount(int replicaCount);

    void Clear();

private:
    struct TChunkPlacementGroupKey
    {
        TConsistentReplicaPlacementHash Hash;
        int MediumIndex;

        operator size_t() const;
        bool operator==(const TChunkPlacementGroupKey& other) const = default;
    };

    class TChunkPlacementGroup
    {
    public:
        explicit TChunkPlacementGroup(TChunk* chunk);

        // Kept sorted for ease of group ordering.
        DEFINE_BYREF_RO_PROPERTY(std::vector<TChunk*>, Chunks);

    public:
        void AddChunk(TChunk* chunk);
        void RemoveChunk(TChunk* chunk, bool missingOk = false);
        bool ContainsChunk(TChunk* chunk) const;
    };

    struct TComparer
    {
        bool operator()(NNodeTrackerServer::TNode* lhs, NNodeTrackerServer::TNode* rhs) const;
        bool operator()(const TChunkPlacementGroup* lhs, const TChunkPlacementGroup* rhs) const;
    };

    struct THasher
    {
        ui64 operator()(NNodeTrackerServer::TNode* node, int index) const;
        ui64 operator()(const TChunkPlacementGroup* group, int index) const;
    };

    int DoAddNode(TNode* node, int mediumIndex, int tokenCount, std::vector<TChunk*>* affectedChunks);
    int DoRemoveNode(TNode* node, int mediumIndex, int tokenCount, std::vector<TChunk*>* affectedChunks);

    const NCellMaster::TBootstrap* Bootstrap_;
    int SufficientlyLargeReplicaCount_;
    THashMap<TChunkPlacementGroupKey, TChunkPlacementGroup> PlacementGroups_;

    THashMap<
        int,
        TConsistentHashingRing<
            NNodeTrackerServer::TNode*,
            const TChunkPlacementGroup*,
            TComparer,
            THasher,
            NChunkClient::TypicalReplicaCount
        >
    > Rings_;
};

DEFINE_REFCOUNTED_TYPE(TConsistentChunkPlacement)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
