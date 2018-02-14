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

#include <yt/core/net/address.h>

#include <yt/core/misc/boolean_formula.h>
#include <yt/core/misc/collection_helpers.h>

#include <atomic>

namespace NYT {
namespace NNodeTrackerServer {

using namespace NNet;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NTabletServer;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

TChunkPtrWithIndexes ToUnapprovedKey(TChunkPtrWithIndexes replica)
{
    auto* chunk = replica.GetPtr();
    auto replicaIndex = chunk->IsJournal() ? GenericChunkReplicaIndex : replica.GetReplicaIndex();
    return TChunkPtrWithIndexes(chunk, replicaIndex, replica.GetMediumIndex());
}

TChunkIdWithIndexes ToRemovalKey(const TChunkIdWithIndexes& replica)
{
    auto replicaIndex = IsJournalChunkId(replica.Id) ? GenericChunkReplicaIndex : replica.ReplicaIndex;
    return TChunkIdWithIndexes(replica.Id, replicaIndex, DefaultStoreMediumIndex);
}

TChunkPtrWithIndexes ToReplicationKey(TChunkPtrWithIndexes replica)
{
    auto* chunk = replica.GetPtr();
    auto replicaIndex = chunk->IsJournal() ? GenericChunkReplicaIndex : replica.GetReplicaIndex();
    return TChunkPtrWithIndexes(chunk, replicaIndex, replica.GetMediumIndex());
}

TChunk* ToSealKey(TChunkPtrWithIndexes replica)
{
    return replica.GetPtr();
}

} // namespace

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
    ChunkReplicationQueues_.resize(ReplicationPriorityCount);
    std::transform(
        Replicas_.begin(),
        Replicas_.end(),
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
    DefaultAddress_ = NNodeTrackerClient::GetDefaultAddress(GetAddressesOrThrow(EAddressType::InternalRpc));
}

void TNode::SetStatistics(NNodeTrackerClient::NProto::TNodeStatistics&& statistics)
{
    Statistics_.Swap(&statistics);
    ComputeFillFactors();
    ComputeSessionCount();
}

void TNode::ComputeFillFactors()
{
    TPerMediumArray<i64> freeSpace{};
    TPerMediumArray<i64> usedSpace{};

    for (const auto& location : Statistics_.locations()) {
        auto mediumIndex = location.medium_index();
        freeSpace[mediumIndex] += std::max(static_cast<i64>(0), location.available_space() - location.low_watermark_space());
        usedSpace[mediumIndex] += location.used_space();
    }

    for (int mediumIndex = 0; mediumIndex < MaxMediumCount; ++mediumIndex) {
        i64 totalSpace = freeSpace[mediumIndex] + usedSpace[mediumIndex];
        FillFactors_[mediumIndex] = (totalSpace == 0)
            ? Null
            : MakeNullable(usedSpace[mediumIndex] / std::max<double>(1.0, totalSpace));
    }
}

void TNode::ComputeSessionCount()
{
    SessionCount_.fill(Null);

    for (const auto& location : Statistics_.locations()) {
        auto mediumIndex = location.medium_index();
        if (location.enabled() && !location.full()) {
            SessionCount_[mediumIndex] = SessionCount_[mediumIndex].Get(0) + location.session_count();
        }
    }
}

TNodeId TNode::GetId() const
{
    return NodeIdFromObjectId(Id_);
}

const TNodeAddressMap& TNode::GetNodeAddresses() const
{
    return NodeAddresses_;
}

void TNode::SetNodeAddresses(const TNodeAddressMap& nodeAddresses)
{
    NodeAddresses_ = nodeAddresses;
    ComputeDefaultAddress();
}

const TAddressMap& TNode::GetAddressesOrThrow(EAddressType addressType) const
{
    return NNodeTrackerClient::GetAddressesOrThrow(NodeAddresses_, addressType);
}

const TString& TNode::GetDefaultAddress() const
{
    return DefaultAddress_;
}

TDataCenter* TNode::GetDataCenter() const
{
    auto* rack = GetRack();
    return rack ? rack->GetDataCenter() : nullptr;
}

bool TNode::HasTag(const TNullable<TString>& tag) const
{
    return !tag || Tags_.find(*tag) != Tags_.end();
}

TNodeDescriptor TNode::GetDescriptor(EAddressType addressType) const
{
    return TNodeDescriptor(
        GetAddressesOrThrow(addressType),
        Rack_ ? MakeNullable(Rack_->GetName()) : Null,
        (Rack_ && Rack_->GetDataCenter()) ? MakeNullable(Rack_->GetDataCenter()->GetName()) : Null,
        std::vector<TString>(Tags_.begin(), Tags_.end()));
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
    auto it = MulticellStates_.find(cellTag);
    YCHECK(it != MulticellStates_.end());
    if (it->second != state) {
        it->second = state;
        ComputeAggregatedState();
    }
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
    Save(context, NodeAddresses_);
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

    // The format is:
    //  (replicaCount, mediumIndex) pairs
    //  0
    int mediumIndex = 0;
    for (const auto& replicas : Replicas_) {
        if (!replicas.empty()) {
            TSizeSerializer::Save(context, replicas.size());
            Save(context, mediumIndex);
        }
        ++mediumIndex;
    }
    TSizeSerializer::Save(context, 0);

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

    // COMPAT(prime)
    if (context.GetVersion() < 610) {
        auto addresses = Load<TAddressMap>(context);
        NodeAddresses_.emplace(EAddressType::InternalRpc, addresses);
    } else {
        Load(context, NodeAddresses_);
    }

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
    // COMPAT(shakurov, babenko)
    if (context.GetVersion() < 400)  {
        YCHECK(TSizeSerializer::Load(context) == 0);
        YCHECK(TSizeSerializer::Load(context) == 0);
    } else if (context.GetVersion() < 401) {
        YCHECK(Load<int>(context) == 0);
        YCHECK(Load<int>(context) == 0);
    } else {
        while (true) {
            auto replicaCount = TSizeSerializer::Load(context);
            if (replicaCount == 0) {
                break;
            }
            auto mediumIndex = Load<int>(context);
            ReserveReplicas(mediumIndex, replicaCount);
        }
    }
    Load(context, UnapprovedReplicas_);
    Load(context, TabletSlots_);

    ComputeDefaultAddress();
}

void TNode::ReserveReplicas(int mediumIndex, int sizeHint)
{
    Replicas_[mediumIndex].reserve(sizeHint);
    RandomReplicaIters_[mediumIndex] = Replicas_[mediumIndex].end();
}

bool TNode::AddReplica(TChunkPtrWithIndexes replica)
{
    auto* chunk = replica.GetPtr();
    if (chunk->IsJournal()) {
        auto mediumIndex = replica.GetMediumIndex();
        DoRemoveReplica(TChunkPtrWithIndexes(chunk, ActiveChunkReplicaIndex, mediumIndex));
        DoRemoveReplica(TChunkPtrWithIndexes(chunk, UnsealedChunkReplicaIndex, mediumIndex));
        DoRemoveReplica(TChunkPtrWithIndexes(chunk, SealedChunkReplicaIndex, mediumIndex));
    }
    // NB: For journal chunks result is always true.
    return DoAddReplica(replica);
}

bool TNode::RemoveReplica(TChunkPtrWithIndexes replica)
{
    auto* chunk = replica.GetPtr();
    if (chunk->IsJournal()) {
        auto mediumIndex = replica.GetMediumIndex();
        DoRemoveReplica(TChunkPtrWithIndexes(chunk, ActiveChunkReplicaIndex, mediumIndex));
        DoRemoveReplica(TChunkPtrWithIndexes(chunk, UnsealedChunkReplicaIndex, mediumIndex));
        DoRemoveReplica(TChunkPtrWithIndexes(chunk, SealedChunkReplicaIndex, mediumIndex));
    } else {
        DoRemoveReplica(replica);
    }
    return UnapprovedReplicas_.erase(ToUnapprovedKey(replica)) == 0;
}

bool TNode::HasReplica(TChunkPtrWithIndexes replica) const
{
    auto* chunk = replica.GetPtr();
    if (chunk->IsJournal()) {
        auto mediumIndex = replica.GetMediumIndex();
        return
            DoHasReplica(TChunkPtrWithIndexes(chunk, ActiveChunkReplicaIndex, mediumIndex)) ||
            DoHasReplica(TChunkPtrWithIndexes(chunk, UnsealedChunkReplicaIndex, mediumIndex)) ||
            DoHasReplica(TChunkPtrWithIndexes(chunk, SealedChunkReplicaIndex, mediumIndex));
    } else {
        return DoHasReplica(replica);
    }
}

TChunkPtrWithIndexes TNode::PickRandomReplica(int mediumIndex)
{
    if (Replicas_[mediumIndex].empty()) {
        return TChunkPtrWithIndexes();
    }

    auto& randomReplicaIt = RandomReplicaIters_[mediumIndex];

    if (randomReplicaIt == Replicas_[mediumIndex].end()) {
        randomReplicaIt = Replicas_[mediumIndex].begin();
    }

    return *(randomReplicaIt++);
}

void TNode::ClearReplicas()
{
    for (auto& replicas : Replicas_) {
        replicas.clear();
    }
    UnapprovedReplicas_.clear();
    for (int index = 0; index < MaxMediumCount; ++index) {
        RandomReplicaIters_[index] = Replicas_[index].end();
    }
}

void TNode::AddUnapprovedReplica(TChunkPtrWithIndexes replica, TInstant timestamp)
{
    YCHECK(UnapprovedReplicas_.emplace(ToUnapprovedKey(replica), timestamp).second);
}

bool TNode::HasUnapprovedReplica(TChunkPtrWithIndexes replica) const
{
    return UnapprovedReplicas_.find(ToUnapprovedKey(replica)) != UnapprovedReplicas_.end();
}

void TNode::ApproveReplica(TChunkPtrWithIndexes replica)
{
    YCHECK(UnapprovedReplicas_.erase(ToUnapprovedKey(replica)) == 1);
    auto* chunk = replica.GetPtr();
    if (chunk->IsJournal()) {
        int mediumIndex = replica.GetMediumIndex();
        DoRemoveReplica(TChunkPtrWithIndexes(chunk, ActiveChunkReplicaIndex, mediumIndex));
        DoRemoveReplica(TChunkPtrWithIndexes(chunk, UnsealedChunkReplicaIndex, mediumIndex));
        DoRemoveReplica(TChunkPtrWithIndexes(chunk, SealedChunkReplicaIndex, mediumIndex));
        YCHECK(DoAddReplica(replica));
    }
}

void TNode::AddToChunkRemovalQueue(const TChunkIdWithIndexes& replica)
{
    Y_ASSERT(GetLocalState() == ENodeState::Online);
    ChunkRemovalQueue_[ToRemovalKey(replica)].set(replica.MediumIndex);
}

void TNode::RemoveFromChunkRemovalQueue(const TChunkIdWithIndexes& replica)
{
    auto key = ToRemovalKey(replica);
    auto it = ChunkRemovalQueue_.find(key);
    if (it != ChunkRemovalQueue_.end()) {
        it->second.reset(replica.MediumIndex);
        if (it->second.none()) {
            ChunkRemovalQueue_.erase(it);
        }
    }
}

void TNode::AddToChunkReplicationQueue(TChunkPtrWithIndexes replica, int targetMediumIndex, int priority)
{
    Y_ASSERT(GetLocalState() == ENodeState::Online);
    ChunkReplicationQueues_[priority][ToReplicationKey(replica)].set(targetMediumIndex);
}

void TNode::RemoveFromChunkReplicationQueues(TChunkPtrWithIndexes replica, int targetMediumIndex)
{
    auto key = ToReplicationKey(replica);
    for (auto& queue : ChunkReplicationQueues_) {
        auto it = queue.find(key);
        if (it != queue.end()) {
            if (targetMediumIndex == AllMediaIndex) {
                queue.erase(it);
            } else {
                it->second.reset(targetMediumIndex);
                if (it->second.none()) {
                    queue.erase(it);
                }
            }
        }
    }
}

void TNode::AddToChunkSealQueue(TChunkPtrWithIndexes chunkWithIndexes)
{
    Y_ASSERT(GetLocalState() == ENodeState::Online);
    auto key = ToSealKey(chunkWithIndexes);
    ChunkSealQueue_[key].set(chunkWithIndexes.GetMediumIndex());
}

void TNode::RemoveFromChunkSealQueue(TChunkPtrWithIndexes chunkWithIndexes)
{
    auto key = ToSealKey(chunkWithIndexes);
    auto it = ChunkSealQueue_.find(key);
    if (it != ChunkSealQueue_.end()) {
        it->second.reset(chunkWithIndexes.GetMediumIndex());
        if (it->second.none()) {
            ChunkSealQueue_.erase(it);
        }
    }
}

void TNode::ClearSessionHints()
{
    HintedUserSessionCount_ .fill(0);
    HintedReplicationSessionCount_.fill(0);
    HintedRepairSessionCount_.fill(0);

    TotalHintedUserSessionCount_ = 0;
    TotalHintedReplicationSessionCount_ = 0;
    TotalHintedRepairSessionCount_ = 0;
}

void TNode::AddSessionHint(int mediumIndex, ESessionType sessionType)
{
    switch (sessionType) {
        case ESessionType::User:
            ++HintedUserSessionCount_[mediumIndex];
            ++TotalHintedUserSessionCount_;
            break;
        case ESessionType::Replication:
            ++HintedReplicationSessionCount_[mediumIndex];
            ++TotalHintedReplicationSessionCount_;
            break;
        case ESessionType::Repair:
            ++HintedRepairSessionCount_[mediumIndex];
            ++TotalHintedRepairSessionCount_;
            break;
        default:
            Y_UNREACHABLE();
    }
}

int TNode::GetHintedSessionCount(int mediumIndex) const
{
    return SessionCount_[mediumIndex].Get(0) +
        HintedUserSessionCount_[mediumIndex] +
        HintedReplicationSessionCount_[mediumIndex] +
        HintedRepairSessionCount_[mediumIndex];
}

int TNode::GetSessionCount(ESessionType sessionType) const
{
    switch (sessionType) {
        case ESessionType::User:
            return Statistics_.total_user_session_count() + TotalHintedUserSessionCount_;
        case ESessionType::Replication:
            return Statistics_.total_replication_session_count() + TotalHintedReplicationSessionCount_;
        case ESessionType::Repair:
            return Statistics_.total_repair_session_count() + TotalHintedRepairSessionCount_;
        default:
            Y_UNREACHABLE();
    }
}

int TNode::GetTotalSessionCount() const
{
    return
        Statistics_.total_user_session_count() + TotalHintedUserSessionCount_ +
        Statistics_.total_replication_session_count() + TotalHintedReplicationSessionCount_ +
        Statistics_.total_repair_session_count() + TotalHintedRepairSessionCount_;
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
        if (ShrinkHashTable(&Replicas_[mediumIndex])) {
            RandomReplicaIters_[mediumIndex] = Replicas_[mediumIndex].end();
        }
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
    LastGossipState_ = ENodeState::Unknown;
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

ui64 TNode::GetVisitMark(int mediumIndex)
{
    return VisitMarks_[mediumIndex];
}

void TNode::SetVisitMark(int mediumIndex, ui64 mark)
{
    VisitMarks_[mediumIndex] = mark;
}

void TNode::ValidateNotBanned()
{
    if (Banned_) {
        THROW_ERROR_EXCEPTION("Node %v is banned", GetDefaultAddress());
    }
}

int TNode::GetTotalTabletSlots() const
{
    return
        Statistics_.used_tablet_slots() +
        Statistics_.available_tablet_slots();
}

bool TNode::HasMedium(int mediumIndex) const
{
    const auto& locations = Statistics_.locations();
    auto it = std::find_if(
        locations.begin(),
        locations.end(),
        [=] (const auto& location) {
            return location.medium_index() == mediumIndex;
        });
    return it != locations.end();
}

TNullable<double> TNode::GetFillFactor(int mediumIndex) const
{
    return FillFactors_[mediumIndex];
}

TNullable<double> TNode::GetLoadFactor(int mediumIndex) const
{
    // NB: Avoid division by zero.
    return SessionCount_[mediumIndex]
        ? MakeNullable(static_cast<double>(GetHintedSessionCount(mediumIndex)) /
            std::max(IOWeights_[mediumIndex], 0.000000001))
        : Null;
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

bool TNode::IsWriteEnabled(int mediumIndex) const
{
    return IOWeights_[mediumIndex] > 0;
}

bool TNode::DoAddReplica(TChunkPtrWithIndexes replica)
{
    auto mediumIndex = replica.GetMediumIndex();
    auto pair = Replicas_[mediumIndex].insert(replica);
    if (pair.second) {
        RandomReplicaIters_[mediumIndex] = pair.first;
        return true;
    } else {
        return false;
    }
}

bool TNode::DoRemoveReplica(TChunkPtrWithIndexes replica)
{
    auto mediumIndex = replica.GetMediumIndex();
    auto& randomReplicaIt = RandomReplicaIters_[mediumIndex];
    auto& mediumStoredReplicas = Replicas_[mediumIndex];
    if (randomReplicaIt != mediumStoredReplicas.end() &&
        *randomReplicaIt == replica)
    {
        ++randomReplicaIt;
    }
    return mediumStoredReplicas.erase(replica) == 1;
}

bool TNode::DoHasReplica(TChunkPtrWithIndexes replica) const
{
    auto mediumIndex = replica.GetMediumIndex();
    return Replicas_[mediumIndex].find(replica) != Replicas_[mediumIndex].end();
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

void TNode::SetNodeTags(const std::vector<TString>& tags)
{
    ValidateNodeTags(tags);
    NodeTags_ = tags;
    RebuildTags();
}

void TNode::SetUserTags(const std::vector<TString>& tags)
{
    ValidateNodeTags(tags);
    UserTags_ = tags;
    RebuildTags();
}

void TNode::RebuildTags()
{
    Tags_.clear();
    Tags_.insert(UserTags_.begin(), UserTags_.end());
    Tags_.insert(NodeTags_.begin(), NodeTags_.end());
    Tags_.insert(TString(GetServiceHostName(GetDefaultAddress())));
    if (Rack_) {
        Tags_.insert(Rack_->GetName());
        if (auto* dc = Rack_->GetDataCenter()) {
            Tags_.insert(dc->GetName());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TNodePtrAddressFormatter::operator()(TStringBuilder* builder, TNode* node) const
{
    builder->AppendString(node->GetDefaultAddress());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
