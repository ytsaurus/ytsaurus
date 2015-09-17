#pragma once

#include "public.h"

#include <core/misc/property.h>
#include <core/misc/nullable.h>

#include <ytlib/node_tracker_client/node_directory.h>
#include <ytlib/node_tracker_client/node_tracker_service.pb.h>

#include <server/hydra/entity_map.h>

#include <server/chunk_server/public.h>
#include <server/chunk_server/chunk_replica.h>

#include <server/transaction_server/public.h>

#include <server/tablet_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ENodeState,
    // Not registered.
    (Offline)
    // Registered but did not report the first heartbeat yet.
    (Registered)
    // Registered and reported the first heartbeat.
    (Online)
    // Known but unregistered, placed into removal queue.
    (Unregistered)
);

class TNode
    : public NHydra::TEntityBase
{
public:
    // Import third-party types into the scope.
    typedef NChunkServer::TChunkPtrWithIndex TChunkPtrWithIndex;
    typedef NChunkServer::TChunkId TChunkId;
    typedef NChunkServer::TChunk TChunk;
    typedef NChunkServer::TJobPtr TJobPtr;

    // Transient properties.
    DEFINE_BYVAL_RW_PROPERTY(ui64, VisitMark);
    DEFINE_BYVAL_RW_PROPERTY(int, LoadRank);
    DEFINE_BYVAL_RW_PROPERTY(double, IOWeight);

    DEFINE_BYVAL_RO_PROPERTY(TNodeId, Id);
    DEFINE_BYVAL_RW_PROPERTY(ENodeState, State);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, RegisterTime);

    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TNodeStatistics, Statistics);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TError>, Alerts);

    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceLimits);
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceUsage);

    DEFINE_BYVAL_RW_PROPERTY(TRack*, Rack);

    // Lease tracking.
    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, Transaction);

    // Chunk Manager stuff.
    DEFINE_BYVAL_RW_PROPERTY(bool, Decommissioned); // kept in sync with |GetConfig()->Decommissioned|.
    DEFINE_BYVAL_RW_PROPERTY(TNullable<NChunkServer::TFillFactorToNodeIterator>, FillFactorIterator);

    // NB: Randomize replica hashing to avoid collisions during balancing.
    using TReplicaSet = yhash_set<TChunkPtrWithIndex>;
    DEFINE_BYREF_RO_PROPERTY(TReplicaSet, StoredReplicas);
    DEFINE_BYREF_RO_PROPERTY(TReplicaSet, CachedReplicas);
    
    //! Maps replicas to the leader timestamp when this replica was registered by a client.
    typedef yhash_map<TChunkPtrWithIndex, TInstant> TUnapprovedReplicaMap;
    DEFINE_BYREF_RW_PROPERTY(TUnapprovedReplicaMap, UnapprovedReplicas);

    DEFINE_BYREF_RW_PROPERTY(yhash_set<TJobPtr>, Jobs);

    //! Indexed by priority.
    typedef std::vector<yhash_set<NChunkServer::TChunkPtrWithIndex>> TChunkReplicationQueues;
    DEFINE_BYREF_RW_PROPERTY(TChunkReplicationQueues, ChunkReplicationQueues);

    typedef yhash_set<NChunkClient::TChunkIdWithIndex> TChunkRemovalQueue;
    DEFINE_BYREF_RW_PROPERTY(TChunkRemovalQueue, ChunkRemovalQueue);

    typedef yhash_set<TChunk*> TChunkSealQueue;
    DEFINE_BYREF_RW_PROPERTY(TChunkSealQueue, ChunkSealQueue);

    // Tablet Manager stuff.
    struct TTabletSlot
    {
        NTabletServer::TTabletCell* Cell = nullptr;
        NHydra::EPeerState PeerState = NHydra::EPeerState::None;
        int PeerId = NHydra::InvalidPeerId;

        void Persist(NCellMaster::TPersistenceContext& context);
    };

    using TTabletSlotList = SmallVector<TTabletSlot, NTabletClient::TypicalCellSize>;
    DEFINE_BYREF_RW_PROPERTY(TTabletSlotList, TabletSlots);

public:
    TNode(
        TNodeId id,
        const TAddressMap& addresses,
        TNodeConfigPtr config,
        TInstant registerTime);
    explicit TNode(TNodeId id);

    TNodeDescriptor GetDescriptor() const;
    const TAddressMap& GetAddresses() const;
    const Stroka& GetDefaultAddress() const;

    const TNodeConfigPtr& GetConfig() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    // Chunk Manager stuff.
    bool AddReplica(TChunkPtrWithIndex replica, bool cached);
    void RemoveReplica(TChunkPtrWithIndex replica, bool cached);
    bool HasReplica(TChunkPtrWithIndex, bool cached) const;
    TChunkPtrWithIndex PickRandomReplica();

    void AddUnapprovedReplica(TChunkPtrWithIndex replica, TInstant timestamp);
    bool HasUnapprovedReplica(TChunkPtrWithIndex replica) const;
    void ApproveReplica(TChunkPtrWithIndex replica);

    void AddToChunkRemovalQueue(const NChunkClient::TChunkIdWithIndex& replica);
    void RemoveFromChunkRemovalQueue(const NChunkClient::TChunkIdWithIndex& replica);
    void ClearChunkRemovalQueue();

    void AddToChunkReplicationQueue(TChunkPtrWithIndex replica, int priority);
    void RemoveFromChunkReplicationQueues(TChunkPtrWithIndex replica);
    void ClearChunkReplicationQueues();

    void AddToChunkSealQueue(TChunk* chunk);
    void RemoveFromChunkSealQueue(TChunk* chunk);
    void ClearChunkSealQueue();

    void ResetSessionHints();
    
    void AddSessionHint(NChunkClient::EWriteSessionType sessionType);

    int GetSessionCount(NChunkClient::EWriteSessionType sessionType) const;
    int GetTotalSessionCount() const;

    int GetTotalTabletSlots() const;

    TTabletSlot* FindTabletSlot(const NTabletServer::TTabletCell* cell);
    TTabletSlot* GetTabletSlot(const NTabletServer::TTabletCell* cell);

    void DetachTabletCell(const NTabletServer::TTabletCell* cell);

    void ShrinkHashTables();

    static ui64 GenerateVisitMark();

private:
    TAddressMap Addresses_;
    const TNodeConfigPtr Config_;

    int HintedUserSessionCount_;
    int HintedReplicationSessionCount_;
    int HintedRepairSessionCount_;

    TReplicaSet::iterator RandomReplicaIt_;

    void Init();

    static TChunkPtrWithIndex ToGeneric(TChunkPtrWithIndex replica);
    static NChunkClient::TChunkIdWithIndex ToGeneric(const NChunkClient::TChunkIdWithIndex& replica);

    bool AddStoredReplica(TChunkPtrWithIndex replica);
    bool RemoveStoredReplica(TChunkPtrWithIndex replica);
    bool ContainsStoredReplica(TChunkPtrWithIndex replica) const;

    bool AddCachedReplica(TChunkPtrWithIndex replica);
    bool RemoveCachedReplica(TChunkPtrWithIndex replica);
    bool ContainsCachedReplica(TChunkPtrWithIndex replica) const;

};

////////////////////////////////////////////////////////////////////////////////

struct TNodePtrAddressFormatter
{
    void operator () (TStringBuilder* builder, TNode* node) const
    {
        builder->AppendString(node->GetDefaultAddress());
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
