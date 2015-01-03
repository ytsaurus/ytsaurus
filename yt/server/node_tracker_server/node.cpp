#include "stdafx.h"
#include "node.h"
#include "rack.h"

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
    const TNodeDescriptor& descriptor,
    TNodeConfigPtr config,
    TInstant registerTime)
    : Id_(id)
    , RegisterTime_(registerTime)
    , Descriptor_(descriptor)
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
    Rack_ = nullptr;
    Transaction_ = nullptr;
    Decommissioned_ = Config_->Decommissioned;
    ChunkReplicationQueues_.resize(ReplicationPriorityCount);
    ResetHints();
}

const TNodeDescriptor& TNode::GetDescriptor() const
{
    return Descriptor_;
}

const Stroka& TNode::GetAddress() const
{
    return Descriptor_.GetDefaultAddress();
}

const TNodeConfigPtr& TNode::GetConfig() const
{
    return Config_;
}

void TNode::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, Descriptor_.Addresses());
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
    Descriptor_ = TNodeDescriptor(Load<TNodeDescriptor::TAddressMap>(context));
    Load(context, State_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 102) {
        Load(context, RegisterTime_);
    }
    Load(context, Statistics_);
    Load(context, Alerts_);
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
        return CachedReplicas_.insert(replica).second;
    } else  {
        if (chunk->IsJournal()) {
            StoredReplicas_.erase(TChunkPtrWithIndex(chunk, ActiveChunkReplicaIndex));
            StoredReplicas_.erase(TChunkPtrWithIndex(chunk, UnsealedChunkReplicaIndex));
            StoredReplicas_.erase(TChunkPtrWithIndex(chunk, SealedChunkReplicaIndex));
        } 
        // NB: For journal chunks result is always true.
        return StoredReplicas_.insert(replica).second;
    }
}

void TNode::RemoveReplica(TChunkPtrWithIndex replica, bool cached)
{
    auto* chunk = replica.GetPtr();
    if (cached) {
        YASSERT(!chunk->IsJournal());
        CachedReplicas_.erase(replica);
    } else {
        if (chunk->IsJournal()) {
            StoredReplicas_.erase(TChunkPtrWithIndex(chunk, ActiveChunkReplicaIndex));
            StoredReplicas_.erase(TChunkPtrWithIndex(chunk, UnsealedChunkReplicaIndex));
            StoredReplicas_.erase(TChunkPtrWithIndex(chunk, SealedChunkReplicaIndex));
        } else {
            StoredReplicas_.erase(replica);
        }

        auto genericReplica = ToGeneric(replica);
        UnapprovedReplicas_.erase(genericReplica);

        ChunkRemovalQueue_.erase(TChunkIdWithIndex(chunk->GetId(), genericReplica.GetIndex()));

        for (auto& queue : ChunkReplicationQueues_) {
            queue.erase(genericReplica);
        }

        ChunkSealQueue_.erase(chunk);
    }
}

bool TNode::HasReplica(TChunkPtrWithIndex replica, bool cached) const
{
    auto* chunk = replica.GetPtr();
    if (cached) {
        YASSERT(!chunk->IsJournal());
        return CachedReplicas_.find(replica) != CachedReplicas_.end();
    } else {
        if (chunk->IsJournal()) {
            return
                StoredReplicas_.find(TChunkPtrWithIndex(chunk, ActiveChunkReplicaIndex)) != StoredReplicas_.end() ||
                StoredReplicas_.find(TChunkPtrWithIndex(chunk, UnsealedChunkReplicaIndex)) != StoredReplicas_.end() ||
                StoredReplicas_.find(TChunkPtrWithIndex(chunk, SealedChunkReplicaIndex)) != StoredReplicas_.end();
        } else {
            return StoredReplicas_.find(replica) != StoredReplicas_.end();
        }
    }
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
        StoredReplicas_.erase(TChunkPtrWithIndex(chunk, ActiveChunkReplicaIndex));
        StoredReplicas_.erase(TChunkPtrWithIndex(chunk, UnsealedChunkReplicaIndex));
        StoredReplicas_.erase(TChunkPtrWithIndex(chunk, SealedChunkReplicaIndex));
        YCHECK(StoredReplicas_.insert(replica).second);
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

void TNode::ResetHints()
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

TNode::TTabletSlot* TNode::FindTabletSlot(TTabletCell* cell)
{
    for (auto& slot : TabletSlots_) {
        if (slot.Cell == cell) {
            return &slot;
        }
    }
    return nullptr;
}

TNode::TTabletSlot* TNode::GetTabletSlot(TTabletCell* cell)
{
    auto* slot = FindTabletSlot(cell);
    YCHECK(slot);
    return slot;
}

void TNode::DetachTabletCell(TTabletCell* cell)
{
    auto* slot = FindTabletSlot(cell);
    if (slot) {
        *slot = TTabletSlot();
    }
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT

