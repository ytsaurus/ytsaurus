#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <ytlib/node_tracker_client/node_directory.h>
#include <ytlib/node_tracker_client/node_tracker_service.pb.h>

#include <ytlib/hydra/public.h>

#include <server/chunk_server/chunk_replica.h>

#include <server/transaction_server/public.h>

#include <server/tablet_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ENodeState,
    // Not registered.
    (Offline)
    // Registered but did not report the first heartbeat yet.
    (Registered)
    // Registered and reported the first heartbeat.
    (Online)
);

class TNode
{
public:
    // Import third-party types into the scope.
    typedef NChunkServer::TChunkPtrWithIndex TChunkPtrWithIndex;
    typedef NChunkServer::TChunkId TChunkId;
    typedef NChunkServer::TChunk TChunk;
    typedef NChunkServer::TJobPtr TJobPtr;

    // Transient properties.
    DEFINE_BYVAL_RW_PROPERTY(bool, UnregisterPending);
    DEFINE_BYVAL_RW_PROPERTY(TAtomic, VisitMark);
    DEFINE_BYVAL_RW_PROPERTY(int, LoadRank);

    DEFINE_BYVAL_RO_PROPERTY(TNodeId, Id);
    DEFINE_BYVAL_RW_PROPERTY(ENodeState, State);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, RegisterTime);

    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TNodeStatistics, Statistics);
    DEFINE_BYREF_RW_PROPERTY(std::vector<Stroka>, Alerts);

    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceLimits);
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceUsage);

    // Lease tracking.
    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, Transaction);

    // Chunk Manager stuff.
    DEFINE_BYVAL_RW_PROPERTY(bool, Decommissioned); // kept in sync with |GetConfig()->Decommissioned|.
    
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TChunkPtrWithIndex>, StoredReplicas);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TChunkPtrWithIndex>, CachedReplicas);
    
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
        int PeerId = -1;

        void Persist(NCellMaster::TPersistenceContext& context);

    };

    typedef SmallVector<TTabletSlot, NTabletClient::TypicalCellSize> TTabletSlotList;
    DEFINE_BYREF_RW_PROPERTY(TTabletSlotList, TabletSlots);

public:
    TNode(
        TNodeId id,
        const TNodeDescriptor& descriptor,
        TNodeConfigPtr config,
        TInstant registerTime);
    explicit TNode(TNodeId id);

    const TNodeDescriptor& GetDescriptor() const;
    const Stroka& GetAddress() const;

    const TNodeConfigPtr& GetConfig() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    // Chunk Manager stuff.
    bool AddReplica(TChunkPtrWithIndex replica, bool cached);
    void RemoveReplica(TChunkPtrWithIndex replica, bool cached);
    bool HasReplica(TChunkPtrWithIndex, bool cached) const;

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

    void ResetHints();
    
    void AddSessionHint(NChunkClient::EWriteSessionType sessionType);

    int GetSessionCount(NChunkClient::EWriteSessionType sessionType) const;
    int GetTotalSessionCount() const;

    int GetTotalTabletSlots() const;

    TTabletSlot* FindTabletSlot(NTabletServer::TTabletCell* cell);
    TTabletSlot* GetTabletSlot(NTabletServer::TTabletCell* cell);

    void DetachTabletCell(NTabletServer::TTabletCell* cell);

    static ui64 GenerateVisitMark();

private:
    TNodeDescriptor Descriptor_;
    TNodeConfigPtr Config_;
    int HintedUserSessionCount_;
    int HintedReplicationSessionCount_;
    int HintedRepairSessionCount_;

    void Init();

    static TChunkPtrWithIndex ToGeneric(TChunkPtrWithIndex replica);
    static NChunkClient::TChunkIdWithIndex ToGeneric(const NChunkClient::TChunkIdWithIndex& replica);

};

////////////////////////////////////////////////////////////////////////////////

struct TNodePtrAddressFormatter
{
    Stroka operator () (TNode* node) const
    {
        return node->GetAddress();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
