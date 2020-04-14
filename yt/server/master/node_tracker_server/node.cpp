#include "node.h"
#include "data_center.h"
#include "rack.h"

#include <yt/server/master/cell_master/serialize.h>

#include <yt/server/master/chunk_server/chunk.h>
#include <yt/server/master/chunk_server/chunk_manager.h>
#include <yt/server/master/chunk_server/job.h>
#include <yt/server/master/chunk_server/medium.h>

#include <yt/server/master/node_tracker_server/config.h>

#include <yt/server/master/cell_server/cell_base.h>

#include <yt/server/master/transaction_server/transaction.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/core/net/address.h>

#include <yt/core/misc/arithmetic_formula.h>
#include <yt/core/misc/collection_helpers.h>

#include <atomic>

namespace NYT::NNodeTrackerServer {

using namespace NNet;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NCellServer;
using namespace NCellMaster;
using namespace NTabletServer;
using namespace NNodeTrackerClient;

using NYT::FromProto;

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

void TNode::TCellSlot::Persist(NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Cell);
    Persist(context, PeerState);
    Persist(context, PeerId);

    if (context.GetVersion() >= EMasterReign::DynamicPeerCount) {
        Persist(context, IsResponseKeeperWarmingUp);
        Persist(context, PreloadPendingStoreCount);
        Persist(context, PreloadCompletedStoreCount);
        Persist(context, PreloadFailedStoreCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

TCellNodeStatistics& operator+=(TCellNodeStatistics& lhs, const TCellNodeStatistics& rhs)
{
    for (const auto& [mediumIndex, chunkReplicaCount] : rhs.ChunkReplicaCount) {
        lhs.ChunkReplicaCount[mediumIndex] += chunkReplicaCount;
    }
    lhs.DestroyedChunkReplicaCount += rhs.DestroyedChunkReplicaCount;
    return lhs;
}

void ToProto(
    NProto::TReqSetCellNodeDescriptors::TStatistics* protoStatistics,
    const TCellNodeStatistics& statistics)
{
    for (const auto& [mediumIndex, replicaCount] : statistics.ChunkReplicaCount) {
        if (replicaCount != 0) {
            auto* mediumStatistics = protoStatistics->add_medium_statistics();
            mediumStatistics->set_medium_index(mediumIndex);
            mediumStatistics->set_chunk_replica_count(replicaCount);
        }
    }
    protoStatistics->set_destroyed_chunk_replica_count(statistics.DestroyedChunkReplicaCount);
}

void FromProto(
    TCellNodeStatistics* statistics,
    const NProto::TReqSetCellNodeDescriptors::TStatistics& protoStatistics)
{
    statistics->ChunkReplicaCount.clear();
    for (const auto& mediumStatistics : protoStatistics.medium_statistics()) {
        auto mediumIndex = mediumStatistics.medium_index();
        auto replicaCount = mediumStatistics.chunk_replica_count();
        statistics->ChunkReplicaCount[mediumIndex] = replicaCount;
    }
    statistics->DestroyedChunkReplicaCount = protoStatistics.destroyed_chunk_replica_count();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TReqSetCellNodeDescriptors::TNodeDescriptor* protoDescriptor, const TCellNodeDescriptor& descriptor)
{
    protoDescriptor->set_state(static_cast<int>(descriptor.State));
    ToProto(protoDescriptor->mutable_statistics(), descriptor.Statistics);
}

void FromProto(TCellNodeDescriptor* descriptor, const NProto::TReqSetCellNodeDescriptors::TNodeDescriptor& protoDescriptor)
{
    descriptor->State = ENodeState(protoDescriptor.state());
    descriptor->Statistics = FromProto<TCellNodeStatistics>(protoDescriptor.statistics());
}

////////////////////////////////////////////////////////////////////////////////

TNode::TNode(TObjectId objectId)
    : TObject(objectId)
{
    ChunkReplicationQueues_.resize(ReplicationPriorityCount);
    ClearSessionHints();
}

void TNode::ComputeAggregatedState()
{
    std::optional<ENodeState> result;
    for (const auto& pair : MulticellDescriptors_) {
        if (result) {
            if (*result != pair.second.State) {
                result = ENodeState::Mixed;
                break;
            }
        } else {
            result = pair.second.State;
        }
    }
    AggregatedState_ = *result;
}

void TNode::ComputeDefaultAddress()
{
    DefaultAddress_ = NNodeTrackerClient::GetDefaultAddress(GetAddressesOrThrow(EAddressType::InternalRpc));
}

void TNode::SetStatistics(
    NNodeTrackerClient::NProto::TNodeStatistics&& statistics,
    const NChunkServer::TChunkManagerPtr& chunkManager)
{
    Statistics_.Swap(&statistics);
    ComputeFillFactors();
    ComputeSessionCount();
    RecomputeIOWeights(chunkManager);
}

void TNode::ComputeFillFactors()
{
    TMediumMap<std::pair<i64, i64>> freeAndUsedSpace;

    for (const auto& location : Statistics_.locations()) {
        auto mediumIndex = location.medium_index();
        auto& space = freeAndUsedSpace[mediumIndex];
        auto& freeSpace = space.first;
        auto& usedSpace = space.second;
        freeSpace += std::max(static_cast<i64>(0), location.available_space() - location.low_watermark_space());
        usedSpace += location.used_space();
    }

    for (const auto& [mediumIndex, space] : freeAndUsedSpace) {
        auto freeSpace = space.first;
        auto usedSpace = space.second;

        i64 totalSpace = freeSpace + usedSpace;
        FillFactors_[mediumIndex] = (totalSpace == 0)
            ? std::nullopt
            : std::make_optional(usedSpace / std::max<double>(1.0, totalSpace));
    }
}

void TNode::ComputeSessionCount()
{
    SessionCount_.clear();
    for (const auto& location : Statistics_.locations()) {
        auto mediumIndex = location.medium_index();
        if (location.enabled() && !location.full()) {
            SessionCount_[mediumIndex] = SessionCount_[mediumIndex].value_or(0) + location.session_count();
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

bool TNode::HasTag(const std::optional<TString>& tag) const
{
    return !tag || Tags_.find(*tag) != Tags_.end();
}

TNodeDescriptor TNode::GetDescriptor(EAddressType addressType) const
{
    return TNodeDescriptor(
        GetAddressesOrThrow(addressType),
        Rack_ ? std::make_optional(Rack_->GetName()) : std::nullopt,
        (Rack_ && Rack_->GetDataCenter()) ? std::make_optional(Rack_->GetDataCenter()->GetName()) : std::nullopt,
        std::vector<TString>(Tags_.begin(), Tags_.end()));
}


void TNode::InitializeStates(TCellTag cellTag, const TCellTagList& secondaryCellTags)
{
    auto addCell = [&] (TCellTag someTag) {
        if (MulticellDescriptors_.find(someTag) == MulticellDescriptors_.end()) {
            YT_VERIFY(MulticellDescriptors_.emplace(someTag, TCellNodeDescriptor{ENodeState::Offline, TCellNodeStatistics()}).second);
        }
    };

    addCell(cellTag);
    for (auto secondaryCellTag : secondaryCellTags) {
        addCell(secondaryCellTag);
    }

    LocalStatePtr_ = &MulticellDescriptors_[cellTag].State;

    ComputeAggregatedState();
}

void TNode::RecomputeIOWeights(const NChunkServer::TChunkManagerPtr& chunkManager)
{
    IOWeights_.clear();
    for (const auto& statistics : Statistics_.media()) {
        auto mediumIndex = statistics.medium_index();
        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        if (!medium || medium->GetCache()) {
            continue;
        }
        IOWeights_[mediumIndex] = statistics.io_weight();
    }
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

        if (state == ENodeState::Unregistered) {
            ClearCellStatistics();
        }
    }
}

void TNode::SetCellDescriptor(TCellTag cellTag, const TCellNodeDescriptor& descriptor)
{
    auto& oldDescriptor = GetOrCrash(MulticellDescriptors_, cellTag);
    auto mustRecomputeState = (oldDescriptor.State != descriptor.State);
    oldDescriptor = descriptor;
    if (mustRecomputeState) {
        ComputeAggregatedState();
    }
}

ENodeState TNode::GetAggregatedState() const
{
    return AggregatedState_;
}

TString TNode::GetLowercaseObjectName() const
{
    return Format("node %v", GetDefaultAddress());
}

TString TNode::GetCapitalizedObjectName() const
{
    return Format("Node %v", GetDefaultAddress());
}

void TNode::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, Banned_);
    Save(context, Decommissioned_);
    Save(context, DisableWriteSessions_);
    Save(context, DisableSchedulerJobs_);
    Save(context, DisableTabletCells_);
    Save(context, NodeAddresses_);
    {
        using TMulticellStates = THashMap<NObjectClient::TCellTag, ENodeState>;
        TMulticellStates multicellStates;
        multicellStates.reserve(MulticellDescriptors_.size());
        for (const auto& pair : MulticellDescriptors_) {
            multicellStates.emplace(pair.first, pair.second.State);
        }

        Save(context, multicellStates);
    }
    Save(context, UserTags_);
    Save(context, NodeTags_);
    Save(context, RegisterTime_);
    Save(context, LastSeenTime_);
    Save(context, Statistics_);
    Save(context, Alerts_);
    Save(context, ResourceLimits_);
    Save(context, ResourceUsage_);
    Save(context, ResourceLimitsOverrides_);
    Save(context, Rack_);
    Save(context, LeaseTransaction_);
    Save(context, DestroyedReplicas_);

    // The is the replica statistics section; the format is as folows:
    // (replicaCount, mediumIndex) for each medium with non-empty set of replicas
    // 0
    {
        SmallVector<int, 8> mediumIndexes;
        for (const auto& [mediumIndex, replicas] : Replicas_) {
            if (!replicas.empty()) {
                mediumIndexes.push_back(mediumIndex);
            }
        }
        std::sort(mediumIndexes.begin(), mediumIndexes.end());
        for (auto mediumIndex : mediumIndexes) {
            const auto& replicas = GetOrCrash(Replicas_, mediumIndex);
            TSizeSerializer::Save(context, replicas.size());
            Save(context, mediumIndex);
        }
        TSizeSerializer::Save(context, 0);
    }

    Save(context, UnapprovedReplicas_);
    Save(context, TabletSlots_);
    Save(context, Annotations_);
    Save(context, Version_);
}

void TNode::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, Banned_);
    Load(context, Decommissioned_);
    Load(context, DisableWriteSessions_);
    Load(context, DisableSchedulerJobs_);
    Load(context, DisableTabletCells_);
    Load(context, NodeAddresses_);

    {
        using TMulticellStates = THashMap<NObjectClient::TCellTag, ENodeState>;
        TMulticellStates multicellStates;
        Load(context, multicellStates);

        MulticellDescriptors_.clear();
        MulticellDescriptors_.reserve(multicellStates.size());
        for (const auto& [cellTag, state] : multicellStates) {
            MulticellDescriptors_.emplace(cellTag, TCellNodeDescriptor{state, TCellNodeStatistics()});
        }
    }

    Load(context, UserTags_);
    Load(context, NodeTags_);
    Load(context, RegisterTime_);
    Load(context, LastSeenTime_);
    Load(context, Statistics_);
    Load(context, Alerts_);
    Load(context, ResourceLimits_);
    Load(context, ResourceUsage_);
    Load(context, ResourceLimitsOverrides_);
    Load(context, Rack_);
    Load(context, LeaseTransaction_);

    // COMPAT(aleksandra-zh)
    if (context.GetVersion() >= EMasterReign::DestroyedChunkRemoval) {
        Load(context, DestroyedReplicas_);
    }

    // NB: This code does not load the replicas per se; it just
    // reserves the appropriate hashtables. Once the snapshot is fully loaded,
    // per-node replica sets get reconstructed from the inverse chunk-to-node mapping.
    // Cf. TNode::Load.
    while (true) {
        auto replicaCount = TSizeSerializer::Load(context);
        if (replicaCount == 0) {
            break;
        }
        auto mediumIndex = Load<int>(context);
        ReserveReplicas(mediumIndex, replicaCount);
    }

    Load(context, UnapprovedReplicas_);
    Load(context, TabletSlots_);
    Load(context, Annotations_);
    Load(context, Version_);

    ComputeDefaultAddress();
}

TJobPtr TNode::FindJob(TJobId jobId)
{
    auto it = IdToJob_.find(jobId);
    return it == IdToJob_.end() ? nullptr : it->second;
}

void TNode::RegisterJob(const TJobPtr& job)
{
    YT_VERIFY(IdToJob_.emplace(job->GetJobId(), job).second);
}

void TNode::UnregisterJob(const TJobPtr& job)
{
    YT_VERIFY(IdToJob_.erase(job->GetJobId()) == 1);
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
    auto it = Replicas_.find(mediumIndex);
    if (it == Replicas_.end() || it->second.empty()) {
        return TChunkPtrWithIndexes();
    }

    auto& randomReplicaIt = RandomReplicaIters_[mediumIndex];

    if (randomReplicaIt == it->second.end()) {
        randomReplicaIt = it->second.begin();
    }

    return *(randomReplicaIt++);
}

void TNode::ClearReplicas()
{
    Replicas_.clear();
    UnapprovedReplicas_.clear();
    RandomReplicaIters_.clear();
    DestroyedReplicas_.clear();
}

void TNode::AddUnapprovedReplica(TChunkPtrWithIndexes replica, TInstant timestamp)
{
    YT_VERIFY(UnapprovedReplicas_.emplace(ToUnapprovedKey(replica), timestamp).second);
}

bool TNode::HasUnapprovedReplica(TChunkPtrWithIndexes replica) const
{
    return UnapprovedReplicas_.find(ToUnapprovedKey(replica)) != UnapprovedReplicas_.end();
}

void TNode::ApproveReplica(TChunkPtrWithIndexes replica)
{
    YT_VERIFY(UnapprovedReplicas_.erase(ToUnapprovedKey(replica)) == 1);
    auto* chunk = replica.GetPtr();
    if (chunk->IsJournal()) {
        int mediumIndex = replica.GetMediumIndex();
        DoRemoveReplica(TChunkPtrWithIndexes(chunk, ActiveChunkReplicaIndex, mediumIndex));
        DoRemoveReplica(TChunkPtrWithIndexes(chunk, UnsealedChunkReplicaIndex, mediumIndex));
        DoRemoveReplica(TChunkPtrWithIndexes(chunk, SealedChunkReplicaIndex, mediumIndex));
        YT_VERIFY(DoAddReplica(replica));
    }
}

bool TNode::AddDestroyedReplica(const TChunkIdWithIndexes& replica)
{
    return DestroyedReplicas_.insert(ToRemovalKey(replica)).second;
}

bool TNode::RemoveDestroyedReplica(const TChunkIdWithIndexes& replica)
{
    return DestroyedReplicas_.erase(ToRemovalKey(replica)) > 0;
}

void TNode::AddToChunkRemovalQueue(const TChunkIdWithIndexes& replica)
{
    YT_ASSERT(GetLocalState() == ENodeState::Online);
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
    YT_ASSERT(GetLocalState() == ENodeState::Online);
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
    YT_ASSERT(GetLocalState() == ENodeState::Online);
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
    HintedUserSessionCount_ .clear();
    HintedReplicationSessionCount_.clear();
    HintedRepairSessionCount_.clear();

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
            YT_ABORT();
    }
}

int TNode::GetHintedSessionCount(int mediumIndex) const
{
    return SessionCount_.lookup(mediumIndex).value_or(0) +
        HintedUserSessionCount_.lookup(mediumIndex) +
        HintedReplicationSessionCount_.lookup(mediumIndex) +
        HintedRepairSessionCount_.lookup(mediumIndex);
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
            YT_ABORT();
    }
}

int TNode::GetTotalSessionCount() const
{
    return
        Statistics_.total_user_session_count() + TotalHintedUserSessionCount_ +
        Statistics_.total_replication_session_count() + TotalHintedReplicationSessionCount_ +
        Statistics_.total_repair_session_count() + TotalHintedRepairSessionCount_;
}

TNode::TCellSlot* TNode::FindCellSlot(const TCellBase* cell)
{
    for (auto& slot : TabletSlots_) {
        if (slot.Cell == cell) {
            return &slot;
        }
    }
    return nullptr;
}

TNode::TCellSlot* TNode::GetCellSlot(const TCellBase* cell)
{
    auto* slot = FindCellSlot(cell);
    YT_VERIFY(slot);
    return slot;
}

void TNode::DetachTabletCell(const TCellBase* cell)
{
    auto* slot = FindCellSlot(cell);
    if (slot) {
        *slot = TCellSlot();
    }
}

void TNode::ShrinkHashTables()
{
    for (auto& [mediumIndex, replicas] : Replicas_) {
        if (ShrinkHashTable(&replicas)) {
            RandomReplicaIters_[mediumIndex] = replicas.end();
        }
    }
    ShrinkHashTable(&UnapprovedReplicas_);
    ShrinkHashTable(&IdToJob_);
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
    IdToJob_.clear();
    ChunkRemovalQueue_.clear();
    for (auto& queue : ChunkReplicationQueues_) {
        queue.clear();
    }
    ChunkSealQueue_.clear();
    FillFactorIterators_.clear();
    LoadFactorIterators_.clear();

    ClearCellStatistics();
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

std::optional<double> TNode::GetFillFactor(int mediumIndex) const
{
    return FillFactors_.lookup(mediumIndex);
}

std::optional<double> TNode::GetLoadFactor(int mediumIndex) const
{
    // NB: Avoid division by zero.
    return SessionCount_.lookup(mediumIndex)
        ? std::make_optional(static_cast<double>(GetHintedSessionCount(mediumIndex)) /
            std::max(IOWeights_.lookup(mediumIndex), 0.000000001))
        : std::nullopt;
}

TNode::TFillFactorIterator TNode::GetFillFactorIterator(int mediumIndex) const
{
    return FillFactorIterators_.lookup(mediumIndex);
}

void TNode::SetFillFactorIterator(int mediumIndex, TFillFactorIterator iter)
{
    FillFactorIterators_[mediumIndex] = iter;
}

TNode::TLoadFactorIterator TNode::GetLoadFactorIterator(int mediumIndex) const
{
    return LoadFactorIterators_.lookup(mediumIndex);
}

void TNode::SetLoadFactorIterator(int mediumIndex, TLoadFactorIterator iter)
{
    LoadFactorIterators_[mediumIndex] = iter;
}

bool TNode::IsWriteEnabled(int mediumIndex) const
{
    return IOWeights_.lookup(mediumIndex) > 0;
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
    if (Replicas_.find(mediumIndex) == Replicas_.end()) {
        return false;
    }
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
    auto it = Replicas_.find(mediumIndex);
    if (it == Replicas_.end()) {
        return false;
    }

    return it->second.find(replica) != it->second.end();
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

void TNode::SetResourceUsage(const NNodeTrackerClient::NProto::TNodeResources& resourceUsage)
{
    ResourceUsage_ = resourceUsage;
}

void TNode::SetResourceLimits(const NNodeTrackerClient::NProto::TNodeResources& resourceLimits)
{
    ResourceLimits_ = resourceLimits;
}

TCellNodeStatistics TNode::ComputeCellStatistics() const
{
    TCellNodeStatistics result = TCellNodeStatistics();
    for (const auto& [mediumIndex, replicas] :  Replicas_) {
        result.ChunkReplicaCount[mediumIndex] = replicas.size();
    }
    result.DestroyedChunkReplicaCount = DestroyedReplicas_.size();
    return result;
}

TCellNodeStatistics TNode::ComputeClusterStatistics() const
{
    // Local (primary) cell statistics aren't stored in MulticellStatistics_.
    TCellNodeStatistics result = ComputeCellStatistics();

    for (const auto& pair : MulticellDescriptors_) {
        result += pair.second.Statistics;
    }
    return result;
}

void TNode::ClearCellStatistics()
{
    for (auto& pair : MulticellDescriptors_) {
        pair.second.Statistics = TCellNodeStatistics();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TNodePtrAddressFormatter::operator()(TStringBuilderBase* builder, TNode* node) const
{
    builder->AppendString(node->GetDefaultAddress());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
