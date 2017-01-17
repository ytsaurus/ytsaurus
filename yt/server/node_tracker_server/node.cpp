#include "node.h"
#include "rack.h"

#include <yt/server/cell_master/serialize.h>

#include <yt/server/chunk_server/chunk.h>
#include <yt/server/chunk_server/job.h>

#include <yt/server/node_tracker_server/config.h>

#include <yt/server/tablet_server/tablet_cell.h>

#include <yt/server/transaction_server/transaction.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/core/misc/collection_helpers.h>
#include <yt/core/misc/address.h>

#include <atomic>

namespace NYT {
namespace NNodeTrackerServer {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NTabletServer;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

void TNode::TTabletSlot::Persist(NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Cell);
    Persist(context, PeerState);
    Persist(context, PeerId);
}

////////////////////////////////////////////////////////////////////////////////

TNode::TNode(const TObjectId& objectId)
    : TObjectBase(objectId)
{
    VisitMark_ = 0;
    IOWeight_ = 0.0;
    Banned_ = false;
    Decommissioned_ = false;
    DisableWriteSessions_ = false;
    Rack_ = nullptr;
    DisableSchedulerJobs_ = false;
    LeaseTransaction_ = nullptr;
    LocalStatePtr_ = nullptr;
    AggregatedState_ = ENodeState::Offline;
    ChunkReplicationQueues_.resize(ReplicationPriorityCount);
    RandomReplicaIt_ = StoredReplicas_.end();
    ClearSessionHints();
}

void TNode::ComputeAggregatedState()
{
    TNullable<ENodeState> result;
    for (const auto& pair : MulticellStates_) {
        if (result) {
            if (*result != pair.second) {
                result = ENodeState::Mixed;
            }
        } else {
            result = pair.second;
        }
    }
    AggregatedState_ = *result;
}

void TNode::ComputeDefaultAddress()
{
    DefaultAddress_ = NNodeTrackerClient::GetDefaultAddress(Addresses_);
}

TNodeId TNode::GetId() const
{
    return NodeIdFromObjectId(Id_);
}

const TAddressMap& TNode::GetAddresses() const
{
    return Addresses_;
}

void TNode::SetAddresses(const TAddressMap& addresses)
{
    Addresses_ = addresses;
    ComputeDefaultAddress();
}

const Stroka& TNode::GetDefaultAddress() const
{
    return DefaultAddress_;
}

TNodeDescriptor TNode::GetDescriptor() const
{
    return TNodeDescriptor(
        Addresses_,
        Rack_ ? MakeNullable(Rack_->GetName()) : Null);
}

void TNode::InitializeStates(TCellTag cellTag, const TCellTagList& secondaryCellTags)
{
    auto addCell = [&] (TCellTag someTag) {
        if (MulticellStates_.find(someTag) == MulticellStates_.end()) {
            YCHECK(MulticellStates_.emplace(someTag, ENodeState::Offline).second);
        }
    };

    addCell(cellTag);
    for (auto secondaryCellTag : secondaryCellTags) {
        addCell(secondaryCellTag);
    }

    LocalStatePtr_ = &MulticellStates_[cellTag];

    ComputeAggregatedState();
}

ENodeState TNode::GetLocalState() const
{
    return *LocalStatePtr_;
}

void TNode::SetLocalState(ENodeState state)
{
    if (*LocalStatePtr_ != state) {
        *LocalStatePtr_ = state;
        ComputeAggregatedState();
    }
}

void TNode::SetState(TCellTag cellTag, ENodeState state)
{
    if (MulticellStates_[cellTag] != state) {
        MulticellStates_[cellTag] = state;
        ComputeAggregatedState();
    }
}

ENodeState TNode::GetAggregatedState() const
{
    return AggregatedState_;
}

std::vector<Stroka> TNode::GetTags() const
{
    std::vector<Stroka> result;

    result.insert(result.end(), UserTags_.begin(), UserTags_.end());
    result.insert(result.end(), NodeTags_.begin(), NodeTags_.end());
    result.push_back(Stroka(GetServiceHostName(GetDefaultAddress())));
    if (Rack_) {
        result.push_back(Rack_->GetName());
    }

    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
}

void TNode::Save(NCellMaster::TSaveContext& context) const
{
    TObjectBase::Save(context);

    using NYT::Save;
    Save(context, Banned_);
    Save(context, Decommissioned_);
    Save(context, DisableWriteSessions_);
    Save(context, DisableSchedulerJobs_);
    Save(context, Addresses_);
    Save(context, MulticellStates_);
    Save(context, UserTags_);
    Save(context, NodeTags_);
    Save(context, RegisterTime_);
    Save(context, LastSeenTime_);
    Save(context, Statistics_);
    Save(context, Alerts_);
    Save(context, ResourceLimitsOverrides_);
    Save(context, Rack_);
    Save(context, LeaseTransaction_);
    TSizeSerializer::Save(context, StoredReplicas_.size());
    TSizeSerializer::Save(context, CachedReplicas_.size());
    Save(context, UnapprovedReplicas_);
    Save(context, TabletSlots_);
}

void TNode::Load(NCellMaster::TLoadContext& context)
{
    // COMPAT(babenko)
    YCHECK(context.GetVersion() >= 208);

    TObjectBase::Load(context);

    using NYT::Load;
    Load(context, Banned_);
    Load(context, Decommissioned_);

    // COMPAT(psushin)
    if (context.GetVersion() >= 353) {
        Load(context, DisableWriteSessions_);
        Load(context, DisableSchedulerJobs_);
    }

    Load(context, Addresses_);
    Load(context, MulticellStates_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 300) {
        Load(context, UserTags_);
        Load(context, NodeTags_);
    }
    Load(context, RegisterTime_);
    Load(context, LastSeenTime_);
    Load(context, Statistics_);
    Load(context, Alerts_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 212) {
        Load(context, ResourceLimitsOverrides_);
    }
    Load(context, Rack_);
    Load(context, LeaseTransaction_);
    ReserveStoredReplicas(TSizeSerializer::Load(context));
    ReserveCachedReplicas(TSizeSerializer::Load(context));
    Load(context, UnapprovedReplicas_);
    Load(context, TabletSlots_);

    ComputeDefaultAddress();
}

void TNode::ReserveStoredReplicas(int sizeHint)
{
    StoredReplicas_.reserve(sizeHint);
    RandomReplicaIt_ = StoredReplicas_.end();
}

void TNode::ReserveCachedReplicas(int sizeHint)
{
    CachedReplicas_.reserve(sizeHint);
}

bool TNode::AddReplica(TChunkPtrWithIndex replica, bool cached)
{
    auto* chunk = replica.GetPtr();
    if (cached) {
        Y_ASSERT(!chunk->IsJournal());
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

bool TNode::RemoveReplica(TChunkPtrWithIndex replica, bool cached)
{
    auto* chunk = replica.GetPtr();
    if (cached) {
        Y_ASSERT(!chunk->IsJournal());
        RemoveCachedReplica(replica);
        return false;
    } else {
        if (chunk->IsJournal()) {
            RemoveStoredReplica(TChunkPtrWithIndex(chunk, ActiveChunkReplicaIndex));
            RemoveStoredReplica(TChunkPtrWithIndex(chunk, UnsealedChunkReplicaIndex));
            RemoveStoredReplica(TChunkPtrWithIndex(chunk, SealedChunkReplicaIndex));
        } else {
            RemoveStoredReplica(replica);
        }
        return UnapprovedReplicas_.erase(ToGeneric(replica)) == 0;
    }
}

bool TNode::HasReplica(TChunkPtrWithIndex replica, bool cached) const
{
    auto* chunk = replica.GetPtr();
    if (cached) {
        Y_ASSERT(!chunk->IsJournal());
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

void TNode::ClearReplicas()
{
    StoredReplicas_.clear();
    CachedReplicas_.clear();
    UnapprovedReplicas_.clear();
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
    Y_ASSERT(GetLocalState() == ENodeState::Online);
    ChunkRemovalQueue_.insert(ToGeneric(replica));
}

void TNode::RemoveFromChunkRemovalQueue(const TChunkIdWithIndex& replica)
{
    ChunkRemovalQueue_.erase(ToGeneric(replica));
}

void TNode::AddToChunkReplicationQueue(TChunkPtrWithIndex replica, int priority)
{
    Y_ASSERT(GetLocalState() == ENodeState::Online);
    ChunkReplicationQueues_[priority].insert(ToGeneric(replica));
}

void TNode::RemoveFromChunkReplicationQueues(TChunkPtrWithIndex replica)
{
    auto genericReplica = ToGeneric(replica);
    for (auto& queue : ChunkReplicationQueues_) {
        queue.erase(genericReplica);
    }
}

void TNode::AddToChunkSealQueue(TChunk* chunk)
{
    Y_ASSERT(GetLocalState() == ENodeState::Online);
    ChunkSealQueue_.insert(chunk);
}

void TNode::RemoveFromChunkSealQueue(TChunk* chunk)
{
    ChunkSealQueue_.erase(chunk);
}

void TNode::ClearSessionHints()
{
    HintedUserSessionCount_ = 0;
    HintedReplicationSessionCount_ = 0;
    HintedRepairSessionCount_ = 0;
}

void TNode::AddSessionHint(ESessionType sessionType)
{
    switch (sessionType) {
        case ESessionType::User:
            ++HintedUserSessionCount_;
            break;
        case ESessionType::Replication:
            ++HintedReplicationSessionCount_;
            break;
        case ESessionType::Repair:
            ++HintedRepairSessionCount_;
            break;
        default:
            YUNREACHABLE();
    }
}

int TNode::GetSessionCount(ESessionType sessionType) const
{
    switch (sessionType) {
        case ESessionType::User:
            return Statistics_.total_user_session_count() + HintedUserSessionCount_;
        case ESessionType::Replication:
            return Statistics_.total_replication_session_count() + HintedReplicationSessionCount_;
        case ESessionType::Repair:
            return Statistics_.total_repair_session_count() + HintedRepairSessionCount_;
        default:
            YUNREACHABLE();
    }
}

int TNode::GetTotalSessionCount() const
{
    return
        Statistics_.total_user_session_count() + HintedUserSessionCount_ +
        Statistics_.total_replication_session_count() + HintedReplicationSessionCount_ +
        Statistics_.total_repair_session_count() + HintedRepairSessionCount_;
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

void TNode::InitTabletSlots()
{
    YCHECK(TabletSlots_.empty());
    TabletSlots_.resize(Statistics_.available_tablet_slots() + Statistics_.used_tablet_slots());
}

void TNode::ClearTabletSlots()
{
    TabletSlots_.clear();
}

void TNode::ShrinkHashTables()
{
    if (ShrinkHashTable(&StoredReplicas_)) {
        RandomReplicaIt_ = StoredReplicas_.end();
    }
    ShrinkHashTable(&CachedReplicas_);
    ShrinkHashTable(&UnapprovedReplicas_);
    ShrinkHashTable(&Jobs_);
    for (auto& queue : ChunkReplicationQueues_) {
        ShrinkHashTable(&queue);
    }
    ShrinkHashTable(&ChunkRemovalQueue_);
    ShrinkHashTable(&ChunkSealQueue_);
}

void TNode::Reset()
{
    ClearSessionHints();
    Jobs_.clear();
    ChunkRemovalQueue_.clear();
    for (auto& queue : ChunkReplicationQueues_) {
        queue.clear();
    }
    ChunkSealQueue_.clear();
    FillFactorIterator_ = Null;
    LoadFactorIterator_ = Null;
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

double TNode::GetFillFactor() const
{
    auto freeSpace = Statistics_.total_available_space() - Statistics_.total_low_watermark_space();
    return
        Statistics_.total_used_space() /
        std::max(1.0, static_cast<double>(freeSpace + Statistics_.total_used_space()));
}

double TNode::GetLoadFactor() const
{
    // NB: Avoid division by zero.
    return static_cast<double>(GetTotalSessionCount()) / std::max(IOWeight_, 0.000000001);
}

bool TNode::IsFull() const
{
    return Statistics_.full();
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

