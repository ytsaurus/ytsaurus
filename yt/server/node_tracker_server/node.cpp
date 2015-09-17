#include "stdafx.h"
#include "node.h"
#include "rack.h"

#include <core/misc/collection_helpers.h>

#include <ytlib/object_client/helpers.h>

#include <server/chunk_server/job.h>
#include <server/chunk_server/chunk.h>

#include <server/transaction_server/transaction.h>

#include <server/tablet_server/tablet_cell.h>

#include <server/node_tracker_server/config.h>

#include <server/cell_master/serialize.h>

#include <atomic>

namespace NYT {
namespace NNodeTrackerServer {

using namespace NObjectClient;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NTabletServer;

////////////////////////////////////////////////////////////////////////////////

void TNode::TTabletSlot::Persist(NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Cell);
    Persist(context, PeerState);
    Persist(context, PeerId);
}

////////////////////////////////////////////////////////////////////////////////

TNode::TNode(
    TNodeId id,
    const TAddressMap& addresses,
    TNodeConfigPtr config,
    TInstant registerTime)
    : Id_(id)
    , RegisterTime_(registerTime)
    , Addresses_(addresses)
    , Config_(config)
{
    Init();
}

TNode::TNode(TNodeId id)
    : Id_(id)
    , Config_(New<TNodeConfig>())
{
    Init();
}

void TNode::Init()
{
    VisitMark_ = 0;
    LoadRank_ = -1;
    IOWeight_ = 0.0;
    Rack_ = nullptr;
    Transaction_ = nullptr;
    Decommissioned_ = Config_->Decommissioned;
    ChunkReplicationQueues_.resize(ReplicationPriorityCount);
    RandomReplicaIt_ = StoredReplicas_.end();
    ResetSessionHints();
}

const TAddressMap& TNode::GetAddresses() const
{
    return Addresses_;
}

const Stroka& TNode::GetDefaultAddress() const
{
    return NNodeTrackerClient::GetDefaultAddress(Addresses_);
}

TNodeDescriptor TNode::GetDescriptor() const
{
    return TNodeDescriptor(
        Addresses_,
        Rack_ ? MakeNullable(Rack_->GetName()) : Null);
}

const TNodeConfigPtr& TNode::GetConfig() const
{
    return Config_;
}

void TNode::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, Addresses_);
    Save(context, State_);
    Save(context, RegisterTime_);
    Save(context, Statistics_);
    Save(context, Alerts_);
    Save(context, Rack_);
    Save(context, Transaction_);
    Save(context, StoredReplicas_);
    Save(context, CachedReplicas_);
    Save(context, UnapprovedReplicas_);
    Save(context, TabletSlots_);
}

void TNode::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, Addresses_);
    Load(context, State_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 102) {
        Load(context, RegisterTime_);
    }
    Load(context, Statistics_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 118) {
        Load(context, Alerts_);
    } else {
        YCHECK(TSizeSerializer::Load(context) == 0);
    }
    // COMPAT(babenko)
    if (context.GetVersion() >= 106) {
        Load(context, Rack_);
    }
    Load(context, Transaction_);
    Load(context, StoredReplicas_);
    Load(context, CachedReplicas_);
    Load(context, UnapprovedReplicas_);
    Load(context, TabletSlots_);
}

bool TNode::AddReplica(TChunkPtrWithIndex replica, bool cached)
{
    auto* chunk = replica.GetPtr();
    if (cached) {
        YASSERT(!chunk->IsJournal());
        return AddCachedReplica(replica);
    } else  {
        if (chunk->IsJournal()) {
            RemoveStoredReplica(TChunkPtrWithIndex(chunk, ActiveChunkReplicaIndex));
            RemoveStoredReplica(TChunkPtrWithIndex(chunk, UnsealedChunkReplicaIndex));
            RemoveStoredReplica(TChunkPtrWithIndex(chunk, SealedChunkReplicaIndex));
        } 
        // NB: For journal chunks result is always true.
        return AddStoredReplica(replica);
    }
}

void TNode::RemoveReplica(TChunkPtrWithIndex replica, bool cached)
{
    auto* chunk = replica.GetPtr();
    if (cached) {
        YASSERT(!chunk->IsJournal());
        RemoveCachedReplica(replica);
    } else {
        if (chunk->IsJournal()) {
            RemoveStoredReplica(TChunkPtrWithIndex(chunk, ActiveChunkReplicaIndex));
            RemoveStoredReplica(TChunkPtrWithIndex(chunk, UnsealedChunkReplicaIndex));
            RemoveStoredReplica(TChunkPtrWithIndex(chunk, SealedChunkReplicaIndex));
        } else {
            RemoveStoredReplica(replica);
        }
        UnapprovedReplicas_.erase(ToGeneric(replica));
    }
}

bool TNode::HasReplica(TChunkPtrWithIndex replica, bool cached) const
{
    auto* chunk = replica.GetPtr();
    if (cached) {
        YASSERT(!chunk->IsJournal());
        return ContainsCachedReplica(replica);
    } else {
        if (chunk->IsJournal()) {
            return
                ContainsStoredReplica(TChunkPtrWithIndex(chunk, ActiveChunkReplicaIndex)) ||
                    ContainsStoredReplica(TChunkPtrWithIndex(chunk, UnsealedChunkReplicaIndex)) ||
                    ContainsStoredReplica(TChunkPtrWithIndex(chunk, SealedChunkReplicaIndex));
        } else {
            return ContainsStoredReplica(replica);
        }
    }
}

TChunkPtrWithIndex TNode::PickRandomReplica()
{
    if (StoredReplicas_.empty()) {
        return TChunkPtrWithIndex();
    }

    if (RandomReplicaIt_ == StoredReplicas_.end()) {
        RandomReplicaIt_ = StoredReplicas_.begin();
    }

    return *(RandomReplicaIt_++);
}

void TNode::AddUnapprovedReplica(TChunkPtrWithIndex replica, TInstant timestamp)
{
    YCHECK(UnapprovedReplicas_.insert(std::make_pair(
        ToGeneric(replica),
        timestamp)).second);
}

bool TNode::HasUnapprovedReplica(TChunkPtrWithIndex replica) const
{
    return
        UnapprovedReplicas_.find(ToGeneric(replica)) !=
        UnapprovedReplicas_.end();
}

void TNode::ApproveReplica(TChunkPtrWithIndex replica)
{
    YCHECK(UnapprovedReplicas_.erase(ToGeneric(replica)) == 1);
    auto* chunk = replica.GetPtr();
    if (chunk->IsJournal()) {
        RemoveStoredReplica(TChunkPtrWithIndex(chunk, ActiveChunkReplicaIndex));
        RemoveStoredReplica(TChunkPtrWithIndex(chunk, UnsealedChunkReplicaIndex));
        RemoveStoredReplica(TChunkPtrWithIndex(chunk, SealedChunkReplicaIndex));
        YCHECK(AddStoredReplica(replica));
    }
}

void TNode::AddToChunkRemovalQueue(const TChunkIdWithIndex& replica)
{
    ChunkRemovalQueue_.insert(ToGeneric(replica));
}

void TNode::RemoveFromChunkRemovalQueue(const TChunkIdWithIndex& replica)
{
    ChunkRemovalQueue_.erase(ToGeneric(replica));
}

void TNode::ClearChunkRemovalQueue()
{
    ChunkRemovalQueue_.clear();
}

void TNode::AddToChunkReplicationQueue(TChunkPtrWithIndex replica, int priority)
{
    ChunkReplicationQueues_[priority].insert(ToGeneric(replica));
}

void TNode::RemoveFromChunkReplicationQueues(TChunkPtrWithIndex replica)
{
    auto genericReplica = ToGeneric(replica);
    for (auto& queue : ChunkReplicationQueues_) {
        queue.erase(genericReplica);
    }
}

void TNode::ClearChunkReplicationQueues()
{
    for (auto& queue : ChunkReplicationQueues_) {
        queue.clear();
    }
}

void TNode::AddToChunkSealQueue(TChunk* chunk)
{
    ChunkSealQueue_.insert(chunk);
}

void TNode::RemoveFromChunkSealQueue(TChunk* chunk)
{
    ChunkSealQueue_.erase(chunk);
}

void TNode::ClearChunkSealQueue()
{
    ChunkSealQueue_.clear();
}

void TNode::ResetSessionHints()
{
    HintedUserSessionCount_ = 0;
    HintedReplicationSessionCount_ = 0;
    HintedRepairSessionCount_ = 0;
}

void TNode::AddSessionHint(EWriteSessionType sessionType)
{
    switch (sessionType) {
        case EWriteSessionType::User:
            ++HintedUserSessionCount_;
            break;
        case EWriteSessionType::Replication:
            ++HintedReplicationSessionCount_;
            break;
        case EWriteSessionType::Repair:
            ++HintedRepairSessionCount_;
            break;
        default:
            YUNREACHABLE();
    }
}

int TNode::GetSessionCount(EWriteSessionType sessionType) const
{
    switch (sessionType) {
        case EWriteSessionType::User:
            return Statistics_.total_user_session_count() + HintedUserSessionCount_;
        case EWriteSessionType::Replication:
            return Statistics_.total_replication_session_count() + HintedReplicationSessionCount_;
        case EWriteSessionType::Repair:
            return Statistics_.total_repair_session_count() + HintedRepairSessionCount_;
        default:
            YUNREACHABLE();
    }
}

int TNode::GetTotalSessionCount() const
{
    return
        GetSessionCount(EWriteSessionType::User) +
        GetSessionCount(EWriteSessionType::Replication) +
        GetSessionCount(EWriteSessionType::Repair);
}

TNode::TTabletSlot* TNode::FindTabletSlot(const TTabletCell* cell)
{
    for (auto& slot : TabletSlots_) {
        if (slot.Cell == cell) {
            return &slot;
        }
    }
    return nullptr;
}

TNode::TTabletSlot* TNode::GetTabletSlot(const TTabletCell* cell)
{
    auto* slot = FindTabletSlot(cell);
    YCHECK(slot);
    return slot;
}

void TNode::DetachTabletCell(const TTabletCell* cell)
{
    auto* slot = FindTabletSlot(cell);
    if (slot) {
        *slot = TTabletSlot();
    }
}

void TNode::ShrinkHashTables()
{
    ShrinkHashTable(&StoredReplicas_);
    RandomReplicaIt_ = StoredReplicas_.end();
    ShrinkHashTable(&CachedReplicas_);
    ShrinkHashTable(&UnapprovedReplicas_);
    ShrinkHashTable(&Jobs_);
    for (auto& queue : ChunkReplicationQueues_) {
        ShrinkHashTable(&queue);
    }
    ShrinkHashTable(&ChunkRemovalQueue_);
    ShrinkHashTable(&ChunkSealQueue_);
}

ui64 TNode::GenerateVisitMark()
{
    static std::atomic<ui64> result(0);
    return ++result;
}

int TNode::GetTotalTabletSlots() const
{
    return
        Statistics_.used_tablet_slots() +
        Statistics_.available_tablet_slots();
}

TChunkPtrWithIndex TNode::ToGeneric(TChunkPtrWithIndex replica)
{
    auto* chunk = replica.GetPtr();
    return chunk->IsJournal()
        ? TChunkPtrWithIndex(chunk, GenericChunkReplicaIndex)
        : replica;
}

TChunkIdWithIndex TNode::ToGeneric(const TChunkIdWithIndex& replica)
{
    return TypeFromId(replica.Id) == EObjectType::JournalChunk
        ? TChunkIdWithIndex(replica.Id, GenericChunkReplicaIndex)
        : replica;
}

bool TNode::AddStoredReplica(TChunkPtrWithIndex replica)
{
    auto pair = StoredReplicas_.insert(replica);
    if (pair.second) {
        RandomReplicaIt_ = pair.first;
        return true;
    } else {
        return false;
    }
}

bool TNode::RemoveStoredReplica(TChunkPtrWithIndex replica)
{
    if (RandomReplicaIt_ != StoredReplicas_.end() && *RandomReplicaIt_ == replica) {
        ++RandomReplicaIt_;
    }
    return StoredReplicas_.erase(replica) == 1;
}

bool TNode::ContainsStoredReplica(TChunkPtrWithIndex replica) const
{
    return StoredReplicas_.find(replica) != StoredReplicas_.end();
}

bool TNode::AddCachedReplica(TChunkPtrWithIndex replica)
{
    return CachedReplicas_.insert(replica).second;
}

bool TNode::RemoveCachedReplica(TChunkPtrWithIndex replica)
{
    return CachedReplicas_.erase(replica) == 1;
}

bool TNode::ContainsCachedReplica(TChunkPtrWithIndex replica) const
{
    return CachedReplicas_.find(replica) != CachedReplicas_.end();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT

