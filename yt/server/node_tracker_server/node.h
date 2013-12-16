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

    // Tablet Manager stuff.
    struct TTabletSlot
    {
        TTabletSlot()
            : Cell(nullptr)
            , PeerState(NHydra::EPeerState::None)
            , PeerId(-1)
        { }

        NTabletServer::TTabletCell* Cell;
        NHydra::EPeerState PeerState;
        int PeerId;

    };

    typedef SmallVector<TTabletSlot, NTabletServer::TypicalCellSize> TTabletSlotList;
    DEFINE_BYREF_RW_PROPERTY(TTabletSlotList, TabletSlots);

    typedef yhash_set<NTabletServer::TTabletCell*> TTabletCellSet;
    DEFINE_BYREF_RW_PROPERTY(TTabletCellSet, TabletCellCreateQueue);

public:
    TNode(
        TNodeId id,
        const TNodeDescriptor& descriptor,
        TNodeConfigPtr config);
    explicit TNode(TNodeId id);

    ~TNode();

    const TNodeDescriptor& GetDescriptor() const;
    const Stroka& GetAddress() const;

    const TNodeConfigPtr& GetConfig() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    // Chunk Manager stuff.
    void AddReplica(TChunkPtrWithIndex replica, bool cached);
    void RemoveReplica(TChunkPtrWithIndex replica, bool cached);
    bool HasReplica(TChunkPtrWithIndex, bool cached) const;

    void MarkReplicaUnapproved(TChunkPtrWithIndex replica, TInstant timestamp);
    bool HasUnapprovedReplica(TChunkPtrWithIndex replica) const;
    void ApproveReplica(TChunkPtrWithIndex replica);

    void ResetHints();
    
    void AddSessionHint(NChunkClient::EWriteSessionType sessionType);
    bool HasSpareSession(NChunkClient::EWriteSessionType sessionType) const;
    int GetTotalSessionCount() const;

    void AddTabletSlotHint();
    int GetTotalUsedTabletSlots() const;

    TTabletSlot* FindTabletSlot(NTabletServer::TTabletCell* cell);
    TTabletSlot* GetTabletSlot(NTabletServer::TTabletCell* cell);

    void DetachTabletCell(NTabletServer::TTabletCell* cell);

    static TAtomic GenerateVisitMark();

private:
    TNodeDescriptor Descriptor_;
    TNodeConfigPtr Config_;
    int HintedUserSessionCount_;
    int HintedReplicationSessionCount_;
    int HintedRepairSessionCount_;
    int HintedTabletSlots_;

    void Init();

};

////////////////////////////////////////////////////////////////////////////////

TNodeId GetObjectId(const TNode* node);
bool CompareObjectsForSerialization(const TNode* lhs, const TNode* rhs);

struct TNodePtrAddressFormatter
{
    Stroka Format(TNode* node) const
    {
        return node->GetAddress();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
