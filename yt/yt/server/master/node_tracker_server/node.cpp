#include "node.h"

#include "config.h"
#include "data_center.h"
#include "host.h"
#include "node_tracker_log.h"
#include "rack.h"
#include "private.h"
#include "rack.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/cell_server/cell_base.h>

#include <yt/yt/server/master/chunk_server/chunk.h>
#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/helpers.h>
#include <yt/yt/server/master/chunk_server/job.h>
#include <yt/yt/server/master/chunk_server/domestic_medium.h>
#include <yt/yt/server/master/chunk_server/chunk_location.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/arithmetic_formula.h>
#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/net/address.h>

#include <atomic>

namespace NYT::NNodeTrackerServer {

using namespace NNet;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NCellarAgent;
using namespace NCellServer;
using namespace NCellMaster;
using namespace NTabletServer;
using namespace NMaintenanceTrackerServer;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NCellarClient;
using namespace NYTree;
using namespace NProfiling;
using namespace NServer;

using NHydra::HasMutationContext;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = NodeTrackerServerLogger;

////////////////////////////////////////////////////////////////////////////////

TIncrementalHeartbeatCounters::TIncrementalHeartbeatCounters(const TProfiler& profiler)
    : RemovedChunks(profiler.Counter("/removed_chunk_count"))
    , RemovedUnapprovedReplicas(profiler.Counter("/removed_unapproved_replica_count"))
    , ApprovedReplicas(profiler.Counter("/approved_replica_count"))
    , AddedReplicas(profiler.Counter("/added_replica_count"))
    , AddedDestroyedReplicas(profiler.Counter("/added_destroyed_replica_count"))
{ }

////////////////////////////////////////////////////////////////////////////////

void TNode::TCellSlot::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Cell);
    Persist(context, PeerState);
    Persist(context, PeerId);
    Persist(context, PreloadPendingStoreCount);
    Persist(context, PreloadCompletedStoreCount);
    Persist(context, PreloadFailedStoreCount);
}

////////////////////////////////////////////////////////////////////////////////

bool TNode::TCellNodeDescriptor::IsReliable() const
{
    return CellReliability == ECellAggregatedStateReliability::StaticallyKnown ||
        CellReliability == ECellAggregatedStateReliability::DynamicallyDiscovered;
}

void TNode::TCellNodeDescriptor::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, State);
    Persist(context, RegistrationPending);

    // COMPAT(cherepashka)
    if (context.IsSave() || context.IsLoad() && context.GetVersion() >= EMasterReign::DynamicMasterCellReconfigurationOnNodes) {
        Persist(context, CellReliability);
    }
}

////////////////////////////////////////////////////////////////////////////////

TCellNodeStatistics& operator+=(TCellNodeStatistics& lhs, const TCellNodeStatistics& rhs)
{
    for (const auto& [mediumIndex, chunkReplicaCount] : rhs.ChunkReplicaCount) {
        lhs.ChunkReplicaCount[mediumIndex] += chunkReplicaCount;
    }
    lhs.DestroyedChunkReplicaCount += rhs.DestroyedChunkReplicaCount;
    lhs.ChunkPushReplicationQueuesSize += rhs.ChunkPushReplicationQueuesSize;
    lhs.ChunkPullReplicationQueuesSize += rhs.ChunkPullReplicationQueuesSize;
    lhs.PullReplicationChunkCount += rhs.PullReplicationChunkCount;
    return lhs;
}

void ToProto(
    NProto::TNodeStatistics* protoStatistics,
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
    protoStatistics->set_chunk_push_replication_queues_size(statistics.ChunkPushReplicationQueuesSize);
    protoStatistics->set_chunk_pull_replication_queues_size(statistics.ChunkPullReplicationQueuesSize);
    protoStatistics->set_pull_replication_chunk_count(statistics.PullReplicationChunkCount);
}

void FromProto(
    TCellNodeStatistics* statistics,
    const NProto::TNodeStatistics& protoStatistics)
{
    statistics->ChunkReplicaCount.clear();
    for (const auto& mediumStatistics : protoStatistics.medium_statistics()) {
        auto mediumIndex = mediumStatistics.medium_index();
        auto replicaCount = mediumStatistics.chunk_replica_count();
        statistics->ChunkReplicaCount[mediumIndex] = replicaCount;
    }
    statistics->DestroyedChunkReplicaCount = protoStatistics.destroyed_chunk_replica_count();
    statistics->ChunkPushReplicationQueuesSize = protoStatistics.chunk_push_replication_queues_size();
    statistics->ChunkPullReplicationQueuesSize = protoStatistics.chunk_pull_replication_queues_size();
    statistics->PullReplicationChunkCount = protoStatistics.pull_replication_chunk_count();
}

////////////////////////////////////////////////////////////////////////////////

TNode::TNode(TObjectId objectId)
    : TObject(objectId)
{
    ChunkPushReplicationQueues_.resize(ReplicationPriorityCount);
    ChunkPullReplicationQueues_.resize(ReplicationPriorityCount);
    ClearSessionHints();
}

int TNode::GetConsistentReplicaPlacementTokenCount(int mediumIndex) const
{
    auto it = ConsistentReplicaPlacementTokenCount_.find(mediumIndex);
    return it == ConsistentReplicaPlacementTokenCount_.end() ? 0 : it->second;
}

void TNode::ComputeAggregatedState()
{
    std::optional<ENodeState> result;
    auto registrationPending = false;
    for (const auto& [cellTag, descriptor] : MulticellDescriptors_) {
        if (!descriptor.IsReliable()) {
            continue;
        }

        if (result) {
            if (*result != descriptor.State) {
                result = ENodeState::Mixed;
                break;
            }
        } else {
            result = descriptor.State;
        }

        registrationPending |= descriptor.RegistrationPending;
    }

    if (AggregatedState_ != *result) {
        AggregatedState_ = *result;
        AggregatedStateChanged_.Fire(this);
    }

    RegistrationPending_ = registrationPending;
}

void TNode::ComputeDefaultAddress()
{
    DefaultAddress_ = NNodeTrackerClient::GetDefaultAddress(GetAddressesOrThrow(EAddressType::InternalRpc));
}

bool TNode::MustReportHeartbeatsToAllMasters() const
{
    const auto& heartbeats = GetHeartbeatTypes();
    return heartbeats.contains(ENodeHeartbeatType::Data) ||
        heartbeats.contains(ENodeHeartbeatType::Tablet) ||
        heartbeats.contains(ENodeHeartbeatType::Cellar);

}

THashSet<ENodeHeartbeatType> TNode::GetHeartbeatTypes() const
{
    THashSet<ENodeHeartbeatType> result;
    result.insert(ENodeHeartbeatType::Cluster);

    for (auto flavor : Flavors()) {
        switch (flavor) {
            case ENodeFlavor::Data:
                result.insert(ENodeHeartbeatType::Data);
                break;

            case ENodeFlavor::Exec:
                if (!GetExecNodeIsNotDataNode()) {
                    result.insert(ENodeHeartbeatType::Data);
                }
                result.insert(ENodeHeartbeatType::Exec);
                break;

            case ENodeFlavor::Tablet:
                result.insert(ENodeHeartbeatType::Tablet);
                result.insert(ENodeHeartbeatType::Cellar);
                break;

            case ENodeFlavor::Chaos:
                result.insert(ENodeHeartbeatType::Cellar);
                break;

            default:
                YT_ABORT();
        }
    }
    return result;
}
bool TNode::IsDataNode() const
{
    return Flavors_.contains(ENodeFlavor::Data);
}

bool TNode::IsExecNode() const
{
    return Flavors_.contains(ENodeFlavor::Exec);
}

bool TNode::IsTabletNode() const
{
    return Flavors_.contains(ENodeFlavor::Tablet);
}

bool TNode::IsChaosNode() const
{
    return Flavors_.contains(ENodeFlavor::Chaos);
}

bool TNode::IsCellarNode() const
{
    return IsTabletNode() || IsChaosNode();
}

bool TNode::ReportedClusterNodeHeartbeat() const
{
    return ReportedHeartbeats_.contains(ENodeHeartbeatType::Cluster);
}

bool TNode::ReportedDataNodeHeartbeat() const
{
    return ReportedHeartbeats_.contains(ENodeHeartbeatType::Data);
}

bool TNode::ReportedExecNodeHeartbeat() const
{
    return ReportedHeartbeats_.contains(ENodeHeartbeatType::Exec);
}

bool TNode::ReportedCellarNodeHeartbeat() const
{
    return ReportedHeartbeats_.contains(ENodeHeartbeatType::Cellar);
}

bool TNode::ReportedTabletNodeHeartbeat() const
{
    return ReportedHeartbeats_.contains(ENodeHeartbeatType::Tablet);
}

void TNode::ValidateRegistered() const
{
    auto state = GetLocalState();
    auto reliability = GetLocalCellAggregatedStateReliability();
    if (state == ENodeState::Registered || state == ENodeState::Online ||
        reliability == ECellAggregatedStateReliability::DuringPropagation)
    {
        return;
    }

    THROW_ERROR_EXCEPTION(NNodeTrackerClient::EErrorCode::InvalidState, "Node is not registered")
        << TErrorAttribute("local_node_state", state);
}

void TNode::SetClusterNodeStatistics(NNodeTrackerClient::NProto::TClusterNodeStatistics&& statistics)
{
    ClusterNodeStatistics_.Swap(&statistics);
}

void TNode::SetExecNodeStatistics(NNodeTrackerClient::NProto::TExecNodeStatistics&& statistics)
{
    ExecNodeStatistics_.Swap(&statistics);
}

void TNode::ComputeFillFactorsAndTotalSpace()
{
    TMediumMap<std::pair<i64, i64>> freeAndUsedSpace;

    for (const auto& location : DataNodeStatistics_.chunk_locations()) {
        auto mediumIndex = location.medium_index();
        auto& space = freeAndUsedSpace[mediumIndex];
        auto& freeSpace = space.first;
        auto& usedSpace = space.second;
        freeSpace += std::max(static_cast<i64>(0), location.available_space() - location.low_watermark_space());
        usedSpace += location.used_space();
    }

    TotalSpace_.clear();

    for (const auto& [mediumIndex, space] : freeAndUsedSpace) {
        auto freeSpace = space.first;
        auto usedSpace = space.second;

        i64 totalSpace = freeSpace + usedSpace;
        FillFactors_[mediumIndex] = (totalSpace == 0)
            ? std::nullopt
            : std::optional(usedSpace / std::max<double>(1.0, totalSpace));
        TotalSpace_[mediumIndex] = totalSpace;
    }
}

void TNode::ComputeSessionCount()
{
    SessionCount_.clear();
    for (const auto& location : DataNodeStatistics_.chunk_locations()) {
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

const std::string& TNode::GetDefaultAddress() const
{
    return DefaultAddress_;
}

TRack* TNode::GetRack() const
{
    auto host = GetHost();
    return host ? host->GetRack() : nullptr;
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
    auto host = GetHost();
    auto* rack = GetRack();
    auto* dataCenter = GetDataCenter();

    return TNodeDescriptor(
        GetAddressesOrThrow(addressType),
        host ? std::optional(host->GetName()) : std::nullopt,
        rack ? std::optional(rack->GetName()) : std::nullopt,
        dataCenter ? std::optional(dataCenter->GetName()) : std::nullopt,
        std::vector(Tags_.begin(), Tags_.end()),
        (GetAggregatedState() == ENodeState::Online) ? std::optional(TInstant::Now()) : std::nullopt);
}

void TNode::InitializeStates(
    TCellTag selfCellTag,
    const std::set<TCellTag>& secondaryCellTags,
    const THashSet<TCellTag>& dynamicallyPropagatedMastersCellTags,
    bool allowMasterCellRemoval)
{
    auto addCell = [&] (TCellTag cellTag) {
        if (!MulticellDescriptors_.contains(cellTag)) {
            auto reliability = dynamicallyPropagatedMastersCellTags.contains(cellTag)
                ? ECellAggregatedStateReliability::DuringPropagation
                : ECellAggregatedStateReliability::StaticallyKnown;

            TCellNodeDescriptor descriptor{
                ENodeState::Offline,
                TCellNodeStatistics(),
                /*registrationPending*/ false,
                reliability,
            };
            EmplaceOrCrash(MulticellDescriptors_, cellTag, descriptor);
        }
    };

    addCell(selfCellTag);
    for (auto secondaryCellTag : secondaryCellTags) {
        addCell(secondaryCellTag);
    }

    for (auto it = MulticellDescriptors_.begin(); it != MulticellDescriptors_.end();) {
        auto cellTag = it->first;
        if (selfCellTag != cellTag && !secondaryCellTags.contains(cellTag)) {
            YT_VERIFY(allowMasterCellRemoval);
            MulticellDescriptors_.erase(it++);
        } else {
            ++it;
        }
    }

    LocalDescriptorPtr_ = &MulticellDescriptors_[selfCellTag];

    ComputeAggregatedState();
}

void TNode::RecomputeIOWeights(const IChunkManagerPtr& chunkManager)
{
    IOWeights_.clear();
    for (const auto& statistics : DataNodeStatistics_.media()) {
        auto mediumIndex = statistics.medium_index();
        if (chunkManager->FindMediumByIndex(mediumIndex)) {
            IOWeights_[mediumIndex] = statistics.io_weight();
        }
    }
}

ENodeState TNode::GetLocalState() const
{
    return LocalDescriptorPtr_ ? LocalDescriptorPtr_->State : ENodeState::Unknown;
}

void TNode::SetLocalState(ENodeState state)
{
    if (LocalDescriptorPtr_->State != state) {
        LocalDescriptorPtr_->State = state;
        ComputeAggregatedState();

        if (state == ENodeState::Unregistered) {
            ClearCellStatistics();
        }
    }
}

ECellAggregatedStateReliability TNode::GetCellAggregatedStateReliability(TCellTag cellTag) const
{
    const auto& descriptor = GetOrCrash(MulticellDescriptors_, cellTag);
    return descriptor.CellReliability;
}

ECellAggregatedStateReliability TNode::GetLocalCellAggregatedStateReliability() const
{
    return LocalDescriptorPtr_? LocalDescriptorPtr_->CellReliability : ECellAggregatedStateReliability::Unknown;
}

void TNode::SetLocalCellAggregatedStateReliability(ECellAggregatedStateReliability reliability)
{
    ValidateReliabilityTransition(LocalDescriptorPtr_->CellReliability, reliability);

    if (LocalDescriptorPtr_->CellReliability != reliability) {
        LocalDescriptorPtr_->CellReliability = reliability;
        ComputeAggregatedState();
    }
}

void TNode::SetRegistrationPending(TCellTag selfCellTag)
{
    YT_VERIFY(HasMutationContext());

    for (auto& [cellTag, descriptor] : MulticellDescriptors_) {
        if (cellTag == selfCellTag) {
            continue;
        }

        if (descriptor.State != ENodeState::Offline) {
            YT_LOG_ALERT("Node is pending registration in unexpected state (NodeId: %v, CellTag: %v, State: %v)",
                GetId(),
                cellTag,
                descriptor.State);
            continue;
        }
        descriptor.RegistrationPending = true;
        RegistrationPending_ = true;
    }
}

void TNode::SetState(
    TCellTag cellTag,
    ENodeState state)
{
    YT_VERIFY(HasMutationContext());

    auto& descriptor = GetOrCrash(MulticellDescriptors_, cellTag);
    auto mustRecomputeState = (descriptor.State != state);

    if (descriptor.State == ENodeState::Offline && state != ENodeState::Offline) {
        descriptor.RegistrationPending = false;
    }

    if (descriptor.RegistrationPending && descriptor.State != ENodeState::Offline) {
        YT_LOG_ALERT("Registration pending flag was not discarded (NodeId: %v, CellTag: %v, OldState: %v, NewState: %v)",
            GetId(),
            cellTag,
            descriptor.State,
            state);
        descriptor.RegistrationPending = false;
    }

    // NB: To remove pending registration flag during replication of node to secondary master.
    if (descriptor.RegistrationPending && state == ENodeState::Offline) {
        descriptor.RegistrationPending = false;
        mustRecomputeState = true;
    }

    descriptor.State = state;

    if (mustRecomputeState) {
        ComputeAggregatedState();
    }
}

void TNode::SetStatistics(
    TCellTag cellTag,
    const TCellNodeStatistics& statistics)
{
    YT_VERIFY(HasMutationContext());

    auto& descriptor = GetOrCrash(MulticellDescriptors_, cellTag);
    descriptor.Statistics = statistics;
}

void TNode::SetCellAggregatedStateReliability(
    TCellTag cellTag,
    ECellAggregatedStateReliability reliability)
{
    YT_VERIFY(HasMutationContext());

    auto& descriptor = GetOrCrash(MulticellDescriptors_, cellTag);

    if (descriptor.CellReliability != reliability) {
        ValidateReliabilityTransition(descriptor.CellReliability, reliability);

        descriptor.CellReliability = reliability;
        ComputeAggregatedState();
    }
}

bool TNode::GetRegistrationPending() const
{
    return RegistrationPending_;
}

ENodeState TNode::GetAggregatedState() const
{
    return AggregatedState_;
}

std::string TNode::GetLowercaseObjectName() const
{
    return Format("node %v", GetDefaultAddress());
}

std::string TNode::GetCapitalizedObjectName() const
{
    return Format("Node %v", GetDefaultAddress());
}

TYPath TNode::GetObjectPath() const
{
    return Format("//sys/cluster_nodes/%v", GetDefaultAddress());
}

void TNode::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);
    TMaintenanceTarget::Save(context);

    using NYT::Save;

    Save(context, NodeAddresses_);
    Save(context, MulticellDescriptors_);
    Save(context, UserTags_);
    Save(context, NodeTags_);
    Save(context, ChunkLocations_);
    Save(context, RegisterTime_);
    Save(context, LastSeenTime_);
    Save(context, ClusterNodeStatistics_);
    Save(context, DataNodeStatistics_);
    Save(context, ExecNodeStatistics_);
    Save(context, JobProxyVersion_);
    Save(context, CellarNodeStatistics_);
    Save(context, Alerts_);
    Save(context, ResourceLimits_);
    Save(context, ResourceUsage_);
    Save(context, ResourceLimitsOverrides_);
    Save(context, Host_);
    Save(context, LeaseTransaction_);
    Save(context, LastSeenLeaseTransactionTimeout_);
    Save(context, Cellars_);
    Save(context, Annotations_);
    Save(context, Version_);
    Save(context, Flavors_);
    Save(context, ReportedHeartbeats_);
    Save(context, ExecNodeIsNotDataNode_);
    Save(context, ConsistentReplicaPlacementTokenCount_);
    Save(context, NextDisposedLocationIndex_);
    Save(context, LastGossipState_);
}

namespace {

// COMPAT(kvk1920): remove after 24.2.
struct TRealChunkLocationPtrSerializer
{
    static void Load(NCellMaster::TLoadContext& context, TChunkLocationRawPtr& location)
    {
        LoadWith<NCellMaster::TRawNonversionedObjectPtrSerializer>(context, location);
    }
};

} // namespace

void TNode::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);
    TMaintenanceTarget::Load(context);

    using NYT::Load;
    Load(context, NodeAddresses_);
    Load(context, MulticellDescriptors_);
    Load(context, UserTags_);
    Load(context, NodeTags_);

    // COMPAT(kvk1920): should be replaced with |Load(context, ChunkLocations_)|
    // after 24.2.
    // NB: In most places |Load<TChunkLocationRawPtr>| means loading of either real or
    // imaginary location. But not here. This is the only place where we are
    // loading |TRealChunkLocationRawptr|.
    TVectorSerializer<TRealChunkLocationPtrSerializer>::Load(context, ChunkLocations_);

    // COMPAT(kvk1920)
    if (context.GetVersion() < EMasterReign::DropImaginaryChunkLocations) {
        auto imaginaryLocationCount = TSizeSerializer::Load(context);

        // Just drop it.
        for (size_t i = 0; i < imaginaryLocationCount; ++i) {
            // Medium index.
            Load<int>(context);

            auto* owningNode = TChunkLocation::SkipImaginaryChunkLocation(context);
            YT_VERIFY(owningNode == this);
        }
    }

    Load(context, RegisterTime_);
    Load(context, LastSeenTime_);
    Load(context, ClusterNodeStatistics_);
    Load(context, DataNodeStatistics_);
    Load(context, ExecNodeStatistics_);
    Load(context, JobProxyVersion_);
    Load(context, CellarNodeStatistics_);
    Load(context, Alerts_);
    Load(context, ResourceLimits_);
    Load(context, ResourceUsage_);
    Load(context, ResourceLimitsOverrides_);
    Load(context, Host_);
    Load(context, LeaseTransaction_);

    if (context.GetVersion() >= EMasterReign::PersistLastSeenLeaseTransactionTimeout ||
        context.GetVersion() < EMasterReign::SecondaryIndex)
    {
        Load(context, LastSeenLeaseTransactionTimeout_);
    }

    Load(context, Cellars_);
    Load(context, Annotations_);
    Load(context, Version_);
    Load(context, Flavors_);
    // COMPAT(savrus) ENodeHeartbeatType is compatible with ENodeFlavor.
    Load(context, ReportedHeartbeats_);
    Load(context, ExecNodeIsNotDataNode_);
    // COMPAT(danilalexeev): YT-23781. Endrsements are now stored at NChunkServer::TChunkLocation.
    if (context.GetVersion() < EMasterReign::PerLocationNodeHeartbeat) {
        TChunkLocation::TEndorsementMap replicaEndorsements;
        Load(context, replicaEndorsements);

        for (auto [chunk, revision] : replicaEndorsements) {
            // For safety reasons, make a location with the highest index responsible
            // for endorsement to prevent endorsing the same chunk twice.
            TChunkLocation* locationWithMaxId = nullptr;
            for (auto replica : chunk->StoredReplicas()) {
                auto* location = replica.GetPtr();
                if (location->GetNode() == this &&
                    (!locationWithMaxId || location->GetId() > locationWithMaxId->GetId()))
                {
                    locationWithMaxId = location;
                }
            }

            if (!locationWithMaxId) {
                YT_LOG_ALERT("Chunk replica endorsement lost during migration"
                    " (ChunkId: %v, Revision: %v)",
                    chunk->GetId(),
                    revision);
                continue;
            }

            EmplaceOrCrash(locationWithMaxId->ReplicaEndorsements(), chunk, revision);
        }
    }
    Load(context, ConsistentReplicaPlacementTokenCount_);
    Load(context, NextDisposedLocationIndex_);
    Load(context, LastGossipState_);

    if (context.GetVersion() < EMasterReign::RemoveStuckAttributes && Attributes_) {
        Attributes_->TryRemove(EInternedAttributeKey::MaintenanceRequests.Unintern());
    }

    ComputeDefaultAddress();
    ComputeFillFactorsAndTotalSpace();
}

TChunkPtrWithReplicaInfo TNode::PickRandomReplica(int mediumIndex)
{
    YT_VERIFY(!HasMutationContext());

    TCompactVector<TChunkLocation*, TypicalChunkLocationCount> feasibleLocations;
    std::copy_if(
        ChunkLocations_.begin(),
        ChunkLocations_.end(),
        std::back_inserter(feasibleLocations),
        [&] (TChunkLocation* location) {
            return
                location->GetEffectiveMediumIndex() == mediumIndex &&
                !location->Replicas().empty();
        });
    if (feasibleLocations.empty()) {
        return TChunkPtrWithReplicaInfo();
    }

    return feasibleLocations[RandomNumber(feasibleLocations.size())]->PickRandomReplica();
}

void TNode::ClearReplicas()
{
    for (auto location : ChunkLocations_) {
        location->ClearReplicas();
    }
}

void TNode::AddToChunkPushReplicationQueue(TChunkIdWithIndex replica, int targetMediumIndex, int priority)
{
    YT_ASSERT(ReportedDataNodeHeartbeat());

    ChunkPushReplicationQueues_[priority].Add(replica, targetMediumIndex);
}

void TNode::AddToChunkPullReplicationQueue(TChunkIdWithIndex replica, int targetMediumIndex, int priority)
{
    YT_ASSERT(ReportedDataNodeHeartbeat());

    ChunkPullReplicationQueues_[priority].Add(replica, targetMediumIndex);
}

void TNode::RefChunkBeingPulled(TChunkId chunkId, int targetMediumIndex)
{
    YT_ASSERT(ReportedDataNodeHeartbeat());
    YT_VERIFY(targetMediumIndex != AllMediaIndex);
    ++ChunksBeingPulled_[chunkId][targetMediumIndex];
}

void TNode::AddTargetReplicationNodeId(TChunkId chunkId, int targetMediumIndex, TNode* node)
{
    YT_ASSERT(ReportedDataNodeHeartbeat());
    if (!PushReplicationTargetNodeIds_[chunkId].emplace(targetMediumIndex, node->GetId()).second) {
        YT_LOG_ALERT("Pull replication is already planned for this chunk to another destination (ChunkId: %v, SourceNodeId: %v, TargetNodeId: %v)",
            chunkId,
            GetId(),
            node->GetId());
    }
}

TNodeId TNode::GetTargetReplicationNodeId(TChunkId chunkId, int targetMediumIndex)
{
    auto it = PushReplicationTargetNodeIds_.find(chunkId);
    if (it == PushReplicationTargetNodeIds_.end()) {
        return InvalidNodeId;
    }

    auto idIt = it->second.find(targetMediumIndex);
    if (idIt == it->second.end()) {
        return InvalidNodeId;
    }
    return idIt->second;
}

void TNode::RemoveTargetReplicationNodeId(TChunkId chunkId, int targetMediumIndex)
{
    auto it = PushReplicationTargetNodeIds_.find(chunkId);
    if (it == PushReplicationTargetNodeIds_.end()) {
        return;
    }

    it->second.erase(targetMediumIndex);
    if (it->second.empty()) {
        PushReplicationTargetNodeIds_.erase(it);
    }
}

void TNode::UnrefChunkBeingPulled(TChunkId chunkId, int targetMediumIndex)
{
    auto chunkIt = ChunksBeingPulled_.find(chunkId);
    if (chunkIt == ChunksBeingPulled_.end()) {
        YT_LOG_ALERT("Trying to remove a chunk from pull replication queue that was already removed (ChunkId: %v, NodeId: %v)",
            chunkId,
            GetId());
        return;
    }

    YT_VERIFY(targetMediumIndex != AllMediaIndex);

    auto mediumIt = chunkIt->second.find(targetMediumIndex);
    if (mediumIt == chunkIt->second.end()) {
        YT_LOG_ALERT("Trying to remove a chunk from pull replication queue that was already removed for that medium (ChunkId: %v, NodeId: %v, Medium: %v)",
            chunkId,
            GetId(),
            targetMediumIndex);
        return;
    }

    if (--mediumIt->second == 0) {
        chunkIt->second.erase(mediumIt);
        if (chunkIt->second.empty()) {
            ChunksBeingPulled_.erase(chunkIt);
        }
    }
}

void TNode::RemoveFromChunkReplicationQueues(TChunkIdWithIndex replica)
{
    for (auto& queue : ChunkPushReplicationQueues_) {
        queue.Erase(replica);
    }

    for (auto& queue : ChunkPullReplicationQueues_) {
        queue.Erase(replica);
    }

    PushReplicationTargetNodeIds_.erase(replica.Id);
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

void TNode::ValidateReliabilityTransition(
    ECellAggregatedStateReliability oldReliability,
    ECellAggregatedStateReliability newReliability) const
{
    auto maybeLogAlertReliability =
        [&] (std::initializer_list<ECellAggregatedStateReliability> allowedReliabilities) {
            if (Find(allowedReliabilities, newReliability) == allowedReliabilities.end()) {
                return;
            }

            YT_LOG_ALERT(
                "Invalid cell aggregated state reliability transition (OldReliability: %v, NewReliability: %v)",
                oldReliability,
                newReliability);
        };

    switch (newReliability) {
        case ECellAggregatedStateReliability::Unknown:
            maybeLogAlertReliability({
                ECellAggregatedStateReliability::StaticallyKnown,
                ECellAggregatedStateReliability::DuringPropagation,
            });
            break;
        case ECellAggregatedStateReliability::StaticallyKnown:
            maybeLogAlertReliability({ECellAggregatedStateReliability::Unknown});
            break;
        case ECellAggregatedStateReliability::DuringPropagation:
            maybeLogAlertReliability({
                ECellAggregatedStateReliability::Unknown,
                ECellAggregatedStateReliability::DynamicallyDiscovered,
            });
            break;
        case ECellAggregatedStateReliability::DynamicallyDiscovered:
            maybeLogAlertReliability({
                ECellAggregatedStateReliability::Unknown,
                ECellAggregatedStateReliability::DuringPropagation,
            });
            break;
        default:
            YT_ABORT();
    }
}

int TNode::GetHintedSessionCount(int mediumIndex, int chunkHostMasterCellCount) const
{
    // Individual chunk host cells are unaware of each other's hinted sessions
    // scheduled to the same node. Take that into account to avoid bursts.
    return GetOrDefault(SessionCount_, mediumIndex).value_or(0) +
        chunkHostMasterCellCount * (
            GetOrDefault(HintedUserSessionCount_, mediumIndex) +
            GetOrDefault(HintedReplicationSessionCount_, mediumIndex) +
            GetOrDefault(HintedRepairSessionCount_, mediumIndex));
}

int TNode::GetTotalSessionCount() const
{
    return
        DataNodeStatistics_.total_user_session_count() + TotalHintedUserSessionCount_ +
        DataNodeStatistics_.total_replication_session_count() + TotalHintedReplicationSessionCount_ +
        DataNodeStatistics_.total_repair_session_count() + TotalHintedRepairSessionCount_;
}

TNode::TCellSlot* TNode::FindCellSlot(const TCellBase* cell)
{
    if (auto* cellar = FindCellar(cell->GetCellarType())) {
        auto predicate = [cell] (const auto& slot) {
            return slot.Cell == cell;
        };

        auto it = std::find_if(cellar->begin(), cellar->end(), predicate);
        if (it != cellar->end()) {
            YT_VERIFY(std::find_if(it + 1, cellar->end(), predicate) == cellar->end());
            return &*it;
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

void TNode::DetachCell(const TCellBase* cell)
{
    if (auto* slot = FindCellSlot(cell)) {
        *slot = TCellSlot();
    }
}

void TNode::ShrinkHashTables()
{
    for (auto& queue : ChunkPushReplicationQueues_) {
        queue.Shrink();
    }
    for (auto& queue : ChunkPullReplicationQueues_) {
        queue.Shrink();
    }
    ShrinkHashTable(ChunksBeingPulled_);
    for (auto location : ChunkLocations_) {
        location->ShrinkHashTables();
    }
}

void TNode::ClearPushReplicationTargetNodeIds(const INodeTrackerPtr& nodeTracker)
{
    for (const auto& [chunkId, mediumToNode] : PushReplicationTargetNodeIds()) {
        for (auto [mediumIndex, nodeId] : mediumToNode) {
            if (auto* node = nodeTracker->FindNode(nodeId)) {
                node->UnrefChunkBeingPulled(chunkId, mediumIndex);
            }
        }
    }
    PushReplicationTargetNodeIds_.clear();
}

void TNode::Reset(const INodeTrackerPtr& nodeTracker)
{
    ClearSessionHints();
    for (auto& queue : ChunkPushReplicationQueues_) {
        queue.clear();
    }
    for (auto& queue : ChunkPullReplicationQueues_) {
        queue.clear();
    }

    // ChunksBeingPulled_ should be cleared by other nodes and jobs.

    ClearPushReplicationTargetNodeIds(nodeTracker);

    // PushReplicationTargetNodeIds_ should be cleared somewhere else
    // (in order to unref chunks being pulled for other nodes).

    LoadFactorIterators_.clear();
    DisableWriteSessionsSentToNode_ = false;
    DisableWriteSessionsReportedByNode_ = false;
    ClearCellStatistics();
    for (auto location : ChunkLocations_) {
        location->Reset();
    }
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

void TNode::SetDataNodeStatistics(
    NNodeTrackerClient::NProto::TDataNodeStatistics&& statistics,
    const IChunkManagerPtr& chunkManager)
{
    DataNodeStatistics_.Swap(&statistics);
    ComputeFillFactorsAndTotalSpace();
    ComputeSessionCount();
    RecomputeIOWeights(chunkManager);
}

void TNode::ValidateNotBanned()
{
    if (IsBanned()) {
        THROW_ERROR_EXCEPTION("Node %v is banned", GetDefaultAddress());
    }
}

bool TNode::HasMedium(int mediumIndex) const
{
    const auto& locations = DataNodeStatistics_.chunk_locations();
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
    return GetOrDefault(FillFactors_, mediumIndex);
}

std::optional<double> TNode::GetLoadFactor(int mediumIndex, int chunkHostMasterCellCount) const
{
    // NB: Avoid division by zero.
    return GetOrDefault(SessionCount_, mediumIndex)
        ? std::optional(
            static_cast<double>(GetHintedSessionCount(mediumIndex, chunkHostMasterCellCount)) /
            std::max(GetOrDefault(IOWeights_, mediumIndex), 1e-9))
        : std::nullopt;
}

TNode::TLoadFactorIterator TNode::GetLoadFactorIterator(int mediumIndex) const
{
    return GetOrDefault(LoadFactorIterators_, mediumIndex);
}

void TNode::SetLoadFactorIterator(int mediumIndex, TLoadFactorIterator iter)
{
    LoadFactorIterators_[mediumIndex] = iter;
}

bool TNode::IsWriteEnabled(int mediumIndex) const
{
    return GetOrDefault(IOWeights_, mediumIndex) > 0;
}

void TNode::SetHost(THost* host)
{
    if (Host_) {
        Host_->RemoveNode(this);
    }

    Host_ = host;

    if (Host_) {
        Host_->AddNode(this);
    }
}

bool TNode::GetEffectiveDisableWriteSessions() const
{
    return AreWriteSessionsDisabled() || DisableWriteSessionsSentToNode_ || DisableWriteSessionsReportedByNode_;
}

void TNode::SetDisableWriteSessionsSentToNode(bool value)
{
    DisableWriteSessionsSentToNode_ = value;
}

void TNode::SetDisableWriteSessionsReportedByNode(bool value)
{
    DisableWriteSessionsReportedByNode_ = value;
}

bool TNode::IsValidWriteTarget() const
{
    // NB: This may be called in mutations so be sure to only rely on persistent state.
    return WasValidWriteTarget(EWriteTargetValidityChange::None);
}

bool TNode::WasValidWriteTarget(EWriteTargetValidityChange reason) const
{
    // NB: This may be called in mutations so be sure to only rely on persistent state.
    auto reportedDataNodeHeartbeat = ReportedDataNodeHeartbeat();
    auto decommissioned = IsDecommissioned();
    auto disableWriteSessions = AreWriteSessionsDisabled();

    switch (reason) {
        case EWriteTargetValidityChange::None:
            break;

        case EWriteTargetValidityChange::ReportedDataNodeHeartbeat:
            reportedDataNodeHeartbeat = !reportedDataNodeHeartbeat;
            break;

        case EWriteTargetValidityChange::Decommissioned:
            decommissioned = !decommissioned;
            break;

        case EWriteTargetValidityChange::WriteSessionsDisabled:
            disableWriteSessions = !disableWriteSessions;
            break;

        default:
            YT_ABORT();
    }

    return
        reportedDataNodeHeartbeat &&
        !decommissioned &&
        !disableWriteSessions;
}

void TNode::SetNodeTags(const std::vector<std::string>& tags)
{
    ValidateNodeTags(tags);
    NodeTags_ = tags;
    RebuildTags();
}

void TNode::SetUserTags(const std::vector<std::string>& tags)
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
    Tags_.insert(std::string(GetServiceHostName(GetDefaultAddress())));
    if (auto* rack = GetRack()) {
        Tags_.insert(rack->GetName());
    }
    if (auto* dataCenter = GetDataCenter()) {
        Tags_.insert(dataCenter->GetName());
    }
    if (auto host = GetHost()) {
        Tags_.insert(host->GetName());
    }
    for (auto flavor : Flavors_) {
        Tags_.insert(Format("flavor:%lv", flavor));
    }
}

void TNode::SetFlavors(const THashSet<ENodeFlavor>& newFlavors)
{
    Flavors_ = newFlavors;
    RebuildTags();
}

void TNode::SetResourceUsage(const NNodeTrackerClient::NProto::TNodeResources& resourceUsage)
{
    ResourceUsage_ = resourceUsage;
}

void TNode::SetResourceLimits(const NNodeTrackerClient::NProto::TNodeResources& resourceLimits)
{
    ResourceLimits_ = resourceLimits;
}

void TNode::InitCellars()
{
    YT_VERIFY(Cellars_.empty());

    for (auto cellarType : TEnumTraits<ECellarType>::GetDomainValues()) {
        if (int size = GetTotalSlotCount(cellarType); size > 0) {
            Cellars_[cellarType].resize(size);
        }
    }
}

void TNode::ClearCellars()
{
    Cellars_.clear();
}

void TNode::UpdateCellarSize(ECellarType cellarType, int newSize)
{
    if (newSize == 0) {
        Cellars_.erase(cellarType);
    } else {
        Cellars_[cellarType].resize(newSize);
    }
}

TNode::TCellar* TNode::FindCellar(ECellarType cellarType)
{
    if (auto it = Cellars_.find(cellarType)) {
        return &it->second;
    }
    return nullptr;
}

const TNode::TCellar* TNode::FindCellar(ECellarType cellarType) const
{
    if (auto it = Cellars_.find(cellarType)) {
        return &it->second;
    }
    return nullptr;
}

TNode::TCellar& TNode::GetCellar(ECellarType cellarType)
{
    auto* cellar = FindCellar(cellarType);
    YT_VERIFY(cellar);
    return *cellar;
}

const TNode::TCellar& TNode::GetCellar(ECellarType cellarType) const
{
    auto* cellar = FindCellar(cellarType);
    YT_VERIFY(cellar);
    return *cellar;
}

int TNode::GetCellarSize(ECellarType cellarType) const
{
    if (auto it = Cellars_.find(cellarType)) {
        return it->second.size();
    }
    return 0;
}

void TNode::SetCellarNodeStatistics(
    ECellarType cellarType,
    TCellarNodeStatistics&& statistics)
{
    CellarNodeStatistics_[cellarType].Swap(&statistics);
}

void TNode::RemoveCellarNodeStatistics(ECellarType cellarType)
{
    CellarNodeStatistics_.erase(cellarType);
}

int TNode::GetAvailableSlotCount(ECellarType cellarType) const
{
    if (const auto& it = CellarNodeStatistics_.find(cellarType)) {
        return it->second.available_cell_slots();
    }

    return 0;
}

int TNode::GetTotalSlotCount(ECellarType cellarType) const
{
    if (const auto& it = CellarNodeStatistics_.find(cellarType)) {
        return
            it->second.used_cell_slots() +
            it->second.available_cell_slots();
    }

    return 0;
}

TCellNodeStatistics TNode::ComputeCellStatistics() const
{
    TCellNodeStatistics result = TCellNodeStatistics();
    for (auto location : ChunkLocations_) {
        result.ChunkReplicaCount[location->GetEffectiveMediumIndex()] += std::ssize(location->Replicas());
        result.DestroyedChunkReplicaCount += location->GetDestroyedReplicasCount();
    }
    for (const auto& queue : ChunkPushReplicationQueues_) {
        result.ChunkPushReplicationQueuesSize += queue.size();
    }
    for (const auto& queue : ChunkPullReplicationQueues_) {
        result.ChunkPullReplicationQueuesSize += queue.size();
    }
    result.PullReplicationChunkCount += std::ssize(ChunksBeingPulled_);
    return result;
}

TCellNodeStatistics TNode::ComputeClusterStatistics() const
{
    // Local (primary) cell statistics aren't stored in MulticellStatistics_.
    TCellNodeStatistics result = ComputeCellStatistics();

    for (const auto& [cellTag, descriptor] : MulticellDescriptors_) {
        if (!descriptor.IsReliable()) {
            continue;
        }

        result += descriptor.Statistics;
    }
    return result;
}

void TNode::ClearCellStatistics()
{
    for (auto& [_, descriptor] : MulticellDescriptors_) {
        descriptor.Statistics = TCellNodeStatistics();
    }
}

i64 TNode::ComputeTotalReplicaCount(int mediumIndex) const
{
    return std::transform_reduce(
        ChunkLocations_.begin(),
        ChunkLocations_.end(),
        i64(0),
        std::plus<i64>{},
        [&] (auto location) {
            if (mediumIndex != AllMediaIndex && mediumIndex != location->GetEffectiveMediumIndex()) {
                return i64(0);
            }
            return std::ssize(location->Replicas());
        });
}

bool TNode::TCellSlot::IsWarmedUp() const
{
    return
        PreloadPendingStoreCount == 0 &&
        PreloadFailedStoreCount == 0 &&
        (PeerState == EPeerState::Leading || PeerState == EPeerState::Following);
}

i64 TNode::ComputeTotalChunkRemovalQueuesSize() const
{
    return std::transform_reduce(
        ChunkLocations_.begin(),
        ChunkLocations_.end(),
        i64(0),
        std::plus<i64>{},
        [] (auto location) {
            return std::ssize(location->ChunkRemovalQueue());
        });
}

i64 TNode::ComputeTotalDestroyedReplicaCount() const
{
    return std::transform_reduce(
        ChunkLocations_.begin(),
        ChunkLocations_.end(),
        0,
        std::plus<i64>{},
        [] (auto location) {
            return location->GetDestroyedReplicasCount();
        });
}

////////////////////////////////////////////////////////////////////////////////

void TNodePtrAddressFormatter::operator()(TStringBuilderBase* builder, TNode* node) const
{
    builder->AppendString(node->GetDefaultAddress());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
