#include "node.h"
#include "data_center.h"
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
    , IOWeights_{}
    , FillFactorIterators_{}
    , LoadFactorIterators_{}
{
    VisitMark_ = 0;
    Banned_ = false;
    Decommissioned_ = false;
    DisableWriteSessions_ = false;
    Rack_ = nullptr;
    DisableSchedulerJobs_ = false;
    LeaseTransaction_ = nullptr;
    LocalStatePtr_ = nullptr;
    AggregatedState_ = ENodeState::Offline;
    ChunkReplicationQueues_.resize(ReplicationPriorityCount);
    std::transform(
        StoredReplicas_.begin(),
        StoredReplicas_.end(),
        RandomReplicaIters_.begin(),
        [] (const TMediumReplicaSet& i) { return i.end(); });
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

bool TNode::HasTag(const TNullable<Stroka>& tag) const
{
    return !tag || Tags_.find(*tag) != Tags_.end();
}

TNodeDescriptor TNode::GetDescriptor() const
{
    return TNodeDescriptor(
        Addresses_,
        Rack_ ? MakeNullable(Rack_->GetName()) : Null,
        (Rack_ && Rack_->GetDataCenter()) ? MakeNullable(Rack_->GetDataCenter()->GetName()) : Null);
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
    *LocalStatePtr_ = state;
    ComputeAggregatedState();
}

void TNode::SetState(TCellTag cellTag, ENodeState state)
{
    MulticellStates_[cellTag] = state;
    ComputeAggregatedState();
}

ENodeState TNode::GetAggregatedState() const
{
    return AggregatedState_;
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

    auto countActiveMedia =
        [] (const TReplicaSet& replicaSet) -> int {
            return std::count_if(
                replicaSet.begin(),
                replicaSet.end(),
                [] (const TMediumReplicaSet& i) { return !i.empty(); });
        };

    auto saveReplicaSetSizes =
        [&] (NCellMaster::TSaveContext& context, const TReplicaSet& replicaSet) {
            // The format is: the number of active media followed by that number
            // of (mediumIndex, replicaCount) pairs.
            Save(context, countActiveMedia(replicaSet));
            int mediumIndex = 0;
            for (const auto& replicas : replicaSet) {
                if (!replicas.empty()) {
                    Save(context, mediumIndex);
                    Save<int>(context, replicas.size());
                }
                ++mediumIndex;
            }
        };

    saveReplicaSetSizes(context, StoredReplicas_);
    saveReplicaSetSizes(context, CachedReplicas_);

    Save(context, UnapprovedReplicas_);
    Save(context, TabletSlots_);
}

void TNode::Load(NCellMaster::TLoadContext& context)
{
    TObjectBase::Load(context);

    using NYT::Load;
    Load(context, Banned_);
    Load(context, Decommissioned_);
    Load(context, DisableWriteSessions_);
    Load(context, DisableSchedulerJobs_);
    Load(context, Addresses_);
    Load(context, MulticellStates_);
    Load(context, UserTags_);
    Load(context, NodeTags_);
    Load(context, RegisterTime_);
    Load(context, LastSeenTime_);
    Load(context, Statistics_);
    Load(context, Alerts_);
    Load(context, ResourceLimitsOverrides_);
    Load(context, Rack_);
    Load(context, LeaseTransaction_);
    // COMPAT(shakurov)
    if (context.GetVersion() < 400)  {
        ReserveReplicas(DefaultStoreMediumIndex, TSizeSerializer::Load(context), false);
        ReserveReplicas(DefaultCacheMediumIndex, TSizeSerializer::Load(context), true);
    } else {
        auto loadReplicaSetSizes =
            [&] (NCellMaster::TLoadContext& context, bool cache) {
                auto activeMediumCount = Load<int>(context);
                for (int i = 0; i < activeMediumCount; ++i) {
                    auto mediumIndex = Load<int>(context);
                    auto replicaCount = Load<int>(context);
                    ReserveReplicas(mediumIndex, replicaCount, cache);
                }
            };

        loadReplicaSetSizes(context, false);
        loadReplicaSetSizes(context, true);
    }
    Load(context, UnapprovedReplicas_);
    Load(context, TabletSlots_);

    ComputeDefaultAddress();
}

void TNode::ReserveReplicas(int mediumIndex, int sizeHint, bool cache)
{
    if (cache) {
        CachedReplicas_[mediumIndex].reserve(sizeHint);
    } else {
        StoredReplicas_[mediumIndex].reserve(sizeHint);
        RandomReplicaIters_[mediumIndex] = StoredReplicas_[mediumIndex].end();
    }
}

bool TNode::AddReplica(TChunkPtrWithIndexes replica, bool cached)
{
    auto* chunk = replica.GetPtr();
    if (cached) {
        Y_ASSERT(!chunk->IsJournal());
        return AddCachedReplica(replica);
    } else  {
        if (chunk->IsJournal()) {
            auto mediumIndex = replica.GetMediumIndex();
            Y_ASSERT(mediumIndex == DefaultStoreMediumIndex);
            RemoveStoredReplica(TChunkPtrWithIndexes(chunk, ActiveChunkReplicaIndex, mediumIndex));
            RemoveStoredReplica(TChunkPtrWithIndexes(chunk, UnsealedChunkReplicaIndex, mediumIndex));
            RemoveStoredReplica(TChunkPtrWithIndexes(chunk, SealedChunkReplicaIndex, mediumIndex));
        }
        // NB: For journal chunks result is always true.
        return AddStoredReplica(replica);
    }
}

bool TNode::RemoveReplica(TChunkPtrWithIndexes replica, bool cached)
{
    auto* chunk = replica.GetPtr();
    if (cached) {
        Y_ASSERT(!chunk->IsJournal());
        RemoveCachedReplica(replica);
        return false;
    } else {
        if (chunk->IsJournal()) {
            auto mediumIndex = replica.GetMediumIndex();
            Y_ASSERT(mediumIndex == DefaultStoreMediumIndex);
            RemoveStoredReplica(TChunkPtrWithIndexes(chunk, ActiveChunkReplicaIndex, mediumIndex));
            RemoveStoredReplica(TChunkPtrWithIndexes(chunk, UnsealedChunkReplicaIndex, mediumIndex));
            RemoveStoredReplica(TChunkPtrWithIndexes(chunk, SealedChunkReplicaIndex, mediumIndex));
        } else {
            RemoveStoredReplica(replica);
        }
        return UnapprovedReplicas_.erase(ToGeneric(replica)) == 0;
    }
}

bool TNode::HasReplica(TChunkPtrWithIndexes replica, bool cached) const
{
    auto* chunk = replica.GetPtr();
    if (cached) {
        Y_ASSERT(!chunk->IsJournal());
        return ContainsCachedReplica(replica);
    } else {
        if (chunk->IsJournal()) {
            auto mediumIndex = replica.GetMediumIndex();
            Y_ASSERT(mediumIndex == DefaultStoreMediumIndex);
            return
                ContainsStoredReplica(TChunkPtrWithIndexes(chunk, ActiveChunkReplicaIndex, mediumIndex)) ||
                ContainsStoredReplica(TChunkPtrWithIndexes(chunk, UnsealedChunkReplicaIndex, mediumIndex)) ||
                ContainsStoredReplica(TChunkPtrWithIndexes(chunk, SealedChunkReplicaIndex, mediumIndex));
        } else {
            return ContainsStoredReplica(replica);
        }
    }
}

TChunkPtrWithIndexes TNode::PickRandomReplica(int mediumIndex)
{
    if (StoredReplicas_[mediumIndex].empty()) {
        return TChunkPtrWithIndexes();
    }

    auto& randomReplicaIt = RandomReplicaIters_[mediumIndex];

    if (randomReplicaIt == StoredReplicas_[mediumIndex].end()) {
        randomReplicaIt = StoredReplicas_[mediumIndex].begin();
    }

    return *(randomReplicaIt++);
}

void TNode::ClearReplicas()
{
    for (auto& replicas : StoredReplicas_) {
        replicas.clear();
    }
    for (auto& replicas : CachedReplicas_) {
        replicas.clear();
    }
    UnapprovedReplicas_.clear();
}

void TNode::AddUnapprovedReplica(TChunkPtrWithIndex replica, TInstant timestamp)
{
    YCHECK(UnapprovedReplicas_.insert(
            std::make_pair(ToGeneric(replica), timestamp)).second);
}

bool TNode::HasUnapprovedReplica(TChunkPtrWithIndex replica) const
{
    return UnapprovedReplicas_.find(ToGeneric(replica)) != UnapprovedReplicas_.end();
}

void TNode::ApproveReplica(TChunkPtrWithIndex replica)
{
    YCHECK(UnapprovedReplicas_.erase(ToGeneric(replica)) == 1);
    auto* chunk = replica.GetPtr();
    if (chunk->IsJournal()) {
        RemoveStoredReplica(TChunkPtrWithIndexes(chunk, ActiveChunkReplicaIndex, DefaultStoreMediumIndex));
        RemoveStoredReplica(TChunkPtrWithIndexes(chunk, UnsealedChunkReplicaIndex, DefaultStoreMediumIndex));
        RemoveStoredReplica(TChunkPtrWithIndexes(chunk, SealedChunkReplicaIndex, DefaultStoreMediumIndex));
        YCHECK(AddStoredReplica({chunk, replica.GetIndex(), DefaultStoreMediumIndex}));
    }
}

void TNode::AddToChunkRemovalQueue(const TChunkIdWithIndexes& replica)
{
    Y_ASSERT(GetLocalState() == ENodeState::Online);
    ChunkRemovalQueue_[ToGeneric(replica)].set(replica.MediumIndex);
}

void TNode::RemoveFromChunkRemovalQueue(const TChunkIdWithIndexes& replica)
{
    auto genericReplica = ToGeneric(replica);
    if (replica.MediumIndex == AllMediaIndex) {
        ChunkRemovalQueue_.erase(genericReplica);
    } else {
        auto it = ChunkRemovalQueue_.find(genericReplica);
        if (it != ChunkRemovalQueue_.end()) {
            it->second.reset(replica.MediumIndex);
            if (it->second.none()) {
                ChunkRemovalQueue_.erase(it);
            }
        }
    }
}

void TNode::AddToChunkReplicationQueue(TChunkPtrWithIndexes replica, int priority)
{
    Y_ASSERT(GetLocalState() == ENodeState::Online);
    ChunkReplicationQueues_[priority][ToGeneric(replica)].set(replica.GetMediumIndex());
}

void TNode::RemoveFromChunkReplicationQueues(TChunkPtrWithIndexes replica)
{
    auto genericReplica = ToGeneric(replica);
    auto mediumIndex = replica.GetMediumIndex();
    if (mediumIndex == AllMediaIndex) {
        for (auto& queue : ChunkReplicationQueues_) {
            queue.erase(genericReplica);
        }
    } else {
        for (auto& queue : ChunkReplicationQueues_) {
            auto it = queue.find(genericReplica);
            if (it != queue.end()) {
                it->second.reset(mediumIndex);
                if (it->second.none()) {
                    queue.erase(it);
                }
            }
        }
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
            Y_UNREACHABLE();
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
            Y_UNREACHABLE();
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
    for (int mediumIndex = 0; mediumIndex < MaxMediumCount; ++mediumIndex) {
        if (ShrinkHashTable(&StoredReplicas_[mediumIndex])) {
            RandomReplicaIters_[mediumIndex] = StoredReplicas_[mediumIndex].end();
        }
    }
    for (auto& mediumCachedReplicas : CachedReplicas_) {
        ShrinkHashTable(&mediumCachedReplicas);
    }
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
    FillFactorIterators_.fill(Null);
    LoadFactorIterators_.fill(Null);
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

double TNode::GetIOWeight(int mediumIndex) const
{
    return IOWeights_[mediumIndex];
}

TNullable<double> TNode::GetFillFactor(int mediumIndex) const
{
    i64 freeSpace = 0;
    i64 usedSpace = 0;

    for (const auto& location : Statistics_.locations()) {
        if (location.medium_index() == mediumIndex) {
            freeSpace += location.available_space() - location.low_watermark_space();
            usedSpace += location.used_space();
        }
    }

    i64 totalSpace = freeSpace + usedSpace;
    if (totalSpace == 0) {
        // No storage of this medium on this node.
        return Null;
    } else {
        return usedSpace / std::max<double>(1.0, totalSpace);
    }
}

TNullable<double> TNode::GetLoadFactor(int mediumIndex) const
{
    // NB: Avoid division by zero.
    return static_cast<double>(GetTotalSessionCount()) / std::max(IOWeights_[mediumIndex], 0.000000001);
}

TNode::TFillFactorIterator TNode::GetFillFactorIterator(int mediumIndex)
{
    return FillFactorIterators_[mediumIndex];
}

void TNode::SetFillFactorIterator(int mediumIndex, TFillFactorIterator iter)
{
    FillFactorIterators_[mediumIndex] = iter;
}

TNode::TLoadFactorIterator TNode::GetLoadFactorIterator(int mediumIndex)
{
    return LoadFactorIterators_[mediumIndex];
}

void TNode::SetLoadFactorIterator(int mediumIndex, TLoadFactorIterator iter)
{
    LoadFactorIterators_[mediumIndex] = iter;
}

bool TNode::IsFull(int mediumIndex) const
{
    bool full = true;
    for (const auto& location : Statistics_.locations()) {
        if (location.medium_index() == mediumIndex && !location.full()) {
            full = false;
            break;
        }
    }
    return full && !Statistics_.locations().empty();
}

TChunkPtrWithIndex TNode::ToGeneric(TChunkPtrWithIndexes replica)
{
    auto* chunk = replica.GetPtr();
    auto replicaIndex = chunk->IsJournal() ? GenericChunkReplicaIndex : replica.GetReplicaIndex();
    return TChunkPtrWithIndex(chunk, replicaIndex);
}

TChunkPtrWithIndex TNode::ToGeneric(TChunkPtrWithIndex replica)
{
    auto* chunk = replica.GetPtr();
    return TChunkPtrWithIndex(chunk, chunk->IsJournal() ? GenericChunkReplicaIndex : replica.GetIndex());
}

TChunkIdWithIndex TNode::ToGeneric(const TChunkIdWithIndexes& replica)
{
    auto replicaIndex = TypeFromId(replica.Id) == EObjectType::JournalChunk
        ? GenericChunkReplicaIndex
        : replica.ReplicaIndex;

    return TChunkIdWithIndex(replica.Id, replicaIndex);
}

bool TNode::AddStoredReplica(TChunkPtrWithIndexes replica)
{
    auto mediumIndex = replica.GetMediumIndex();
    auto pair = StoredReplicas_[mediumIndex].insert(replica);
    if (pair.second) {
        RandomReplicaIters_[mediumIndex] = pair.first;
        return true;
    } else {
        return false;
    }
}

bool TNode::RemoveStoredReplica(TChunkPtrWithIndexes replica)
{
    auto mediumIndex = replica.GetMediumIndex();
    auto& randomReplicaIt = RandomReplicaIters_[mediumIndex];
    auto& mediumStoredReplicas = StoredReplicas_[mediumIndex];
    if (randomReplicaIt != mediumStoredReplicas.end() &&
        *randomReplicaIt == replica)
    {
        ++randomReplicaIt;
    }
    return mediumStoredReplicas.erase(replica) == 1;
}

bool TNode::ContainsStoredReplica(TChunkPtrWithIndexes replica) const
{
    auto mediumIndex = replica.GetMediumIndex();
    return StoredReplicas_[mediumIndex].find(replica) != StoredReplicas_[mediumIndex].end();
}

bool TNode::AddCachedReplica(TChunkPtrWithIndexes replica)
{
    return CachedReplicas_[replica.GetMediumIndex()].insert(replica).second;
}

bool TNode::RemoveCachedReplica(TChunkPtrWithIndexes replica)
{
    return CachedReplicas_[replica.GetMediumIndex()].erase(replica) == 1;
}

bool TNode::ContainsCachedReplica(TChunkPtrWithIndexes replica) const
{
    auto mediumCachedReplicas = CachedReplicas_[replica.GetMediumIndex()];
    return mediumCachedReplicas.find(replica) != mediumCachedReplicas.end();
}

void TNode::SetRack(TRack* rack)
{
    Rack_ = rack;
    RebuildTags();
}

void TNode::SetBanned(bool value)
{
    Banned_ = value;
}

void TNode::SetDecommissioned(bool value)
{
    Decommissioned_ = value;
}

void TNode::SetDisableWriteSessions(bool value)
{
    DisableWriteSessions_ = value;
}

void TNode::SetNodeTags(const std::vector<Stroka>& tags)
{
    NodeTags_ = tags;
    RebuildTags();
}

void TNode::SetUserTags(const std::vector<Stroka>& tags)
{
    UserTags_ = tags;
    RebuildTags();
}

void TNode::RebuildTags()
{
    Tags_.clear();
    Tags_.insert(UserTags_.begin(), UserTags_.end());
    Tags_.insert(NodeTags_.begin(), NodeTags_.end());
    Tags_.insert(Stroka(GetServiceHostName(GetDefaultAddress())));
    if (Rack_) {
        Tags_.insert(Rack_->GetName());
        if (auto* dc = Rack_->GetDataCenter()) {
            Tags_.insert(dc->GetName());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
