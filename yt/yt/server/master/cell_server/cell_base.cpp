#include "cell_base.h"

#include "area.h"
#include "cell_bundle.h"
#include "helpers.h"

#include <yt/yt/server/master/tablet_server/tablet.h>

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/lib/cell_server/proto/cell_manager.pb.h>

#include <yt/yt/server/lib/hydra/hydra_context.h>

#include <yt/yt/server/lib/cellar_agent/helpers.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NCellServer {

using namespace NCellMaster;
using namespace NCellarAgent;
using namespace NCellarClient;
using namespace NHiveClient;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NTabletClient;
using namespace NTabletServer;
using namespace NYTree;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

// COMPAT(savrus)
struct TTabletCellConfig
    : public NYTree::TYsonStruct
{
    std::vector<std::optional<std::string>> Addresses;

    REGISTER_YSON_STRUCT(TTabletCellConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("addresses", &TThis::Addresses);
    }
};

////////////////////////////////////////////////////////////////////////////////

void TCellStatus::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Decommissioned);
    PHOENIX_REGISTER_FIELD(2, Health);
}

PHOENIX_DEFINE_TYPE(TCellStatus);

void ToProto(NProto::TCellStatus* protoStatus, const TCellStatus& status)
{
    protoStatus->set_decommissioned(status.Decommissioned);
    protoStatus->set_health(ToProto(status.Health));
}

void FromProto(TCellStatus* status, const NProto::TCellStatus& protoStatus)
{
    using NYT::FromProto;

    status->Decommissioned = protoStatus.decommissioned();
    status->Health = FromProto<ECellHealth>(protoStatus.health());
}

void Serialize(const TCellStatus& status, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("health").Value(status.Health)
            .Item("decommissioned").Value(status.Decommissioned)
        .EndMap();
}

void Serialize(const TCellHealthHistoryItem& item, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("time").Value(item.Time)
            .Item("health").Value(item.Health)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void TCellBase::TPeer::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Descriptor);
    PHOENIX_REGISTER_FIELD(2, Node);
    PHOENIX_REGISTER_FIELD(3, LastSeenTime);
    PHOENIX_REGISTER_FIELD(4, LastRevocationReason);
    PHOENIX_REGISTER_FIELD(5, LastSeenState);
    PHOENIX_REGISTER_FIELD(6, PrerequisiteTransaction);
}

PHOENIX_DEFINE_TYPE(TCellBase::TPeer);

////////////////////////////////////////////////////////////////////////////////

TCellBase::TCellBase(TTamedCellId id)
    : TObject(id)
    , ShardIndex_(GetCellShardIndex(id))
{ }

void TCellBase::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, LeadingPeerId_);
    PHOENIX_REGISTER_FIELD(2, Peers_);
    PHOENIX_REGISTER_FIELD(3, ConfigVersion_);
    PHOENIX_REGISTER_FIELD(4, PrerequisiteTransaction_);
    PHOENIX_REGISTER_FIELD(5, CellBundle_);
    PHOENIX_REGISTER_FIELD(6, Area_);
    PHOENIX_REGISTER_FIELD(7, CellLifeStage_);
    PHOENIX_REGISTER_FIELD(8, GossipStatus_);
    PHOENIX_REGISTER_FIELD(9, PeerCount_);
    PHOENIX_REGISTER_FIELD(10, LastLeaderChangeTime_);
    PHOENIX_REGISTER_FIELD(11, Suspended_);
    PHOENIX_REGISTER_FIELD(12, LeaseTransactionIds_);
    PHOENIX_REGISTER_FIELD(13, RegisteredInCypress_);
    PHOENIX_REGISTER_FIELD(14, PendingAclsUpdate_);
    PHOENIX_REGISTER_FIELD(15, MaxSnapshotId_);
    PHOENIX_REGISTER_FIELD(16, MaxChangelogId_);
}

PHOENIX_DEFINE_TYPE(TCellBase);

int TCellBase::FindPeerId(const std::string& address) const
{
    for (auto peerId = 0; peerId < std::ssize(Peers_); ++peerId) {
        const auto& peer = Peers_[peerId];
        if (peer.Descriptor.GetDefaultAddress() == address) {
            return peerId;
        }
    }
    return InvalidPeerId;
}

int TCellBase::GetPeerId(const std::string& address) const
{
    auto peerId = FindPeerId(address);
    YT_VERIFY(peerId != InvalidPeerId);
    return peerId;
}

int TCellBase::FindPeerId(TNode* node) const
{
    for (int peerId = 0; peerId < std::ssize(Peers_); ++peerId) {
        if (Peers_[peerId].Node == node) {
            return peerId;
        }
    }
    return InvalidPeerId;
}

int TCellBase::GetPeerId(TNode* node) const
{
    auto peerId = FindPeerId(node);
    YT_VERIFY(peerId != InvalidPeerId);
    return peerId;
}

void TCellBase::AssignPeer(const TCellPeerDescriptor& descriptor, int peerId)
{
    auto& peer = Peers_[peerId];
    YT_VERIFY(peer.Descriptor.IsNull());
    YT_VERIFY(!descriptor.IsNull());
    peer.Descriptor = descriptor;
}

void TCellBase::RevokePeer(int peerId, const TError& reason)
{
    auto& peer = Peers_[peerId];
    YT_VERIFY(!peer.Descriptor.IsNull());
    peer.Descriptor = TCellPeerDescriptor();
    peer.Node = nullptr;
    peer.LastRevocationReason = reason;
}

void TCellBase::ExpireDiagnosticErrors(TInstant deadline)
{
    auto clearError = [&] (TError* error) {
        if (!error->IsOK() && error->GetDatetime() < deadline) {
            *error = {};
        }
    };

    for (auto& peer : Peers_) {
        clearError(&peer.LastRevocationReason);
        clearError(&peer.LastHydraRestartReason);
    }
}

void TCellBase::AttachPeer(TNode* node, int peerId)
{
    auto& peer = Peers_[peerId];
    YT_VERIFY(peer.Descriptor.GetDefaultAddress() == node->GetDefaultAddress());

    YT_VERIFY(!peer.Node);
    peer.Node = node;
}

void TCellBase::DetachPeer(TNode* node)
{
    auto peerId = FindPeerId(node);
    if (peerId != InvalidPeerId) {
        Peers_[peerId].Node = nullptr;
    }
}

void TCellBase::UpdatePeerSeenTime(int peerId, TInstant when)
{
    auto& peer = Peers_[peerId];
    peer.LastSeenTime = when;
}

void TCellBase::UpdatePeerState(int peerId, EPeerState peerState)
{
    auto& peer = Peers_[peerId];
    if (peerId == GetLeadingPeerId() && peer.LastSeenState != EPeerState::Leading && peerState == EPeerState::Leading) {
        const auto* hydraContext = GetCurrentHydraContext();
        LastLeaderChangeTime_ = hydraContext->GetTimestamp();
    }
    peer.LastSeenState = peerState;
}

TCellSlot* TCellBase::FindCellSlot(int peerId) const
{
    auto node = Peers_[peerId].Node;
    if (!node) {
        return nullptr;
    }

    return node->FindCellSlot(this);
}

EPeerState TCellBase::GetPeerState(int peerId) const
{
    auto* slot = FindCellSlot(peerId);
    if (!slot) {
        return EPeerState::None;
    }

    return slot->PeerState;
}

NTransactionServer::TTransaction* TCellBase::GetPrerequisiteTransaction(std::optional<int> peerId) const
{
    if (IsIndependent()) {
        return Peers()[*peerId].PrerequisiteTransaction;
    } else {
        return GetPrerequisiteTransaction();
    }
}

void TCellBase::SetPrerequisiteTransaction(std::optional<int> peerId, NTransactionServer::TTransaction* transaction)
{
    if (IsIndependent()) {
        Peers()[*peerId].PrerequisiteTransaction = transaction;
    } else {
        SetPrerequisiteTransaction(transaction);
    }
}

int TCellBase::GetDescriptorConfigVersion() const
{
    return ConfigVersion_;
}

bool TCellBase::IsAlienPeer(int /*peerId*/) const
{
    return false;
}

ECellHealth TCellBase::GetHealth() const
{
    return IsIndependent()
        ? GetCumulativeIndependentPeersHealth()
        : GetCumulativeDependentPeersHealth();
}

ECellHealth TCellBase::GetCumulativeIndependentPeersHealth() const
{
    for (auto peerId = 0; peerId < std::ssize(Peers_); ++peerId) {
        if (IsAlienPeer(peerId)) {
            continue;
        }

        const auto& peer = Peers_[peerId];
        auto node = peer.Node;
        if (!IsObjectAlive(node)) {
            return ETabletCellHealth::Degraded;
        }
        const auto* slot = node->GetCellSlot(this);
        if (slot->PeerState != EPeerState::Following && slot->PeerState != EPeerState::Leading) {
            return ETabletCellHealth::Degraded;
        }
    }

    return ETabletCellHealth::Good;
}

ECellHealth TCellBase::GetCumulativeDependentPeersHealth() const
{
    const auto& leaderPeer = Peers_[LeadingPeerId_];
    auto leaderNode = leaderPeer.Node;
    if (!IsObjectAlive(leaderNode)) {
        return ECellHealth::Failed;
    }

    const auto* leaderSlot = leaderNode->GetCellSlot(this);
    if (leaderSlot->PeerState != EPeerState::Leading) {
        return ECellHealth::Failed;
    }

    for (auto peerId = 0; peerId < std::ssize(Peers_); ++peerId) {
        if (peerId == LeadingPeerId_) {
            continue;
        }
        const auto& peer = Peers_[peerId];
        auto node = peer.Node;
        if (!IsObjectAlive(node)) {
            return ECellHealth::Degraded;
        }
        const auto* slot = node->GetCellSlot(this);
        if (slot->PeerState != EPeerState::Following) {
            return ECellHealth::Degraded;
        }
    }

    return ECellHealth::Good;
}

bool TCellBase::IsHealthy() const
{
    const auto& leaderPeer = Peers_[LeadingPeerId_];
    auto leaderNode = leaderPeer.Node;
    if (!IsObjectAlive(leaderNode)) {
        return false;
    }

    const auto* leaderSlot = leaderNode->GetCellSlot(this);
    if (leaderSlot->PeerState != EPeerState::Leading) {
        return false;
    }

    return true;
}

ECellHealth TCellBase::GetMulticellHealth() const
{
    return CombineHealths(GetHealth(), GossipStatus().Cluster().Health);
}

void TCellBase::RecomputeClusterStatus()
{
    GossipStatus().Cluster() = TCellStatus();
    GossipStatus().Cluster().Decommissioned = true;
    GossipStatus().Cluster().Health = GetHealth();
    for (const auto& [cellTag, status] : GossipStatus().Multicell()) {
        GossipStatus().Cluster().Decommissioned &= status.Decommissioned;
        GossipStatus().Cluster().Health = CombineHealths(GossipStatus().Cluster().Health, status.Health);
    }
}

void TCellBase::UpdateHealthHistory(
    ECellHealth oldHealth,
    ECellHealth newHealth,
    int healthHistoryMaxSize,
    TDuration healthHistoryExpirationTime)
{
    auto now = GetCurrentMutationContext()->GetTimestamp();
    auto expirationDeadline = now - healthHistoryExpirationTime;
    while (HealthHistory_.size() > 1 && HealthHistory_.front().Time < expirationDeadline) {
        HealthHistory_.pop_front();
    }

    if (oldHealth != newHealth && healthHistoryMaxSize > 0) {
        HealthHistory_.push_back({
            .Time = now,
            .Health = newHealth,
        });
    }

    while (std::ssize(HealthHistory_) > healthHistoryMaxSize) {
        HealthHistory_.pop_front();
    }
}

ECellHealth TCellBase::CombineHealths(ECellHealth lhs, ECellHealth rhs)
{
    static constexpr std::array<ECellHealth, 4> HealthOrder{{
        ECellHealth::Failed,
        ECellHealth::Degraded,
        ECellHealth::Initializing,
        ECellHealth::Good,
    }};

    for (auto health : HealthOrder) {
        if (lhs == health || rhs == health) {
            return health;
        }
    }

    return ECellHealth::Failed;
}

bool TCellBase::IsDecommissionStarted() const
{
    return CellLifeStage_ == ECellLifeStage::DecommissioningOnMaster ||
        CellLifeStage_ == ECellLifeStage::DecommissioningOnNode ||
        CellLifeStage_ == ECellLifeStage::Decommissioned;
}

bool TCellBase::IsDecommissionCompleted() const
{
    return CellLifeStage_ == ECellLifeStage::Decommissioned;
}

bool TCellBase::IsIndependent() const
{
    return CellBundle()->GetOptions()->IndependentPeers;
}

ECellarType TCellBase::GetCellarType() const
{
    return GetCellarTypeFromCellId(GetId());
}

bool TCellBase::IsValidPeer(int peerId) const
{
    return 0 <= peerId && peerId < std::ssize(Peers_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
