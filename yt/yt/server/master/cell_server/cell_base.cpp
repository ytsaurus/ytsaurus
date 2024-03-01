#include "cell_base.h"

#include "area.h"
#include "cell_bundle.h"
#include "helpers.h"

#include <yt/yt/server/master/tablet_server/tablet.h>

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/lib/cell_server/proto/cell_manager.pb.h>

#include <yt/yt/server/lib/hydra/hydra_context.h>

#include <yt/yt/server/lib/cellar_agent/helpers.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NCellServer {

using namespace NCellMaster;
using namespace NCellarAgent;
using namespace NCellarClient;
using namespace NHiveClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NTabletClient;
using namespace NTabletServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

// COMPAT(savrus)
class TTabletCellConfig
    : public NYTree::TYsonStruct
{
public:
    std::vector<std::optional<TString>> Addresses;

    REGISTER_YSON_STRUCT(TTabletCellConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("addresses", &TThis::Addresses);
    }
};

////////////////////////////////////////////////////////////////////////////////

void TCellStatus::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Decommissioned);
    Persist(context, Health);
}

void ToProto(NProto::TCellStatus* protoStatus, const TCellStatus& status)
{
    protoStatus->set_decommissioned(status.Decommissioned);
    protoStatus->set_health(static_cast<int>(status.Health));
}

void FromProto(TCellStatus* status, const NProto::TCellStatus& protoStatus)
{
    status->Decommissioned = protoStatus.decommissioned();
    status->Health = ECellHealth(protoStatus.health());
}

void Serialize(const TCellStatus& status, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("health").Value(status.Health)
            .Item("decommissioned").Value(status.Decommissioned)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void TCellBase::TPeer::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Descriptor);
    Persist(context, Node);
    Persist(context, LastSeenTime);
    Persist(context, LastRevocationReason);
    Persist(context, LastSeenState);
    Persist(context, PrerequisiteTransaction);
}

////////////////////////////////////////////////////////////////////////////////

TCellBase::TCellBase(TTamedCellId id)
    : TObject(id)
    , ShardIndex_(GetCellShardIndex(id))
{ }

void TCellBase::Save(TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, LeadingPeerId_);
    Save(context, Peers_);
    Save(context, ConfigVersion_);
    Save(context, PrerequisiteTransaction_);
    Save(context, CellBundle_);
    Save(context, Area_);
    Save(context, CellLifeStage_);
    Save(context, GossipStatus_);
    Save(context, PeerCount_);
    Save(context, LastLeaderChangeTime_);
    Save(context, Suspended_);
    Save(context, LeaseTransactionIds_);
    Save(context, RegisteredInCypress_);
    Save(context, PendingAclsUpdate_);
    Save(context, MaxSnapshotId_);
    Save(context, MaxChangelogId_);
}

void TCellBase::Load(TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, LeadingPeerId_);
    Load(context, Peers_);
    Load(context, ConfigVersion_);
    Load(context, PrerequisiteTransaction_);
    Load(context, CellBundle_);
    Load(context, Area_);
    Load(context, CellLifeStage_);
    Load(context, GossipStatus_);
    Load(context, PeerCount_);
    Load(context, LastLeaderChangeTime_);
    Load(context, Suspended_);

    // COMPAT(gritukan)
    if (context.GetVersion() >= EMasterReign::TabletPrerequisites) {
        Load(context, LeaseTransactionIds_);
    }

    // COMPAT(danilalexeev)
    if (context.GetVersion() >= EMasterReign::TabletCellsHydraPersistenceMigration) {
        Load(context, RegisteredInCypress_);
        Load(context, PendingAclsUpdate_);
    }

    // COMPAT(ifsmirnov)
    if (context.GetVersion() >= EMasterReign::CachedMaxSnapshotId) {
        Load(context, MaxSnapshotId_);
        Load(context, MaxChangelogId_);
    }
}

int TCellBase::FindPeerId(const TString& address) const
{
    for (auto peerId = 0; peerId < std::ssize(Peers_); ++peerId) {
        const auto& peer = Peers_[peerId];
        if (peer.Descriptor.GetDefaultAddress() == address) {
            return peerId;
        }
    }
    return InvalidPeerId;
}

int TCellBase::GetPeerId(const TString& address) const
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

void TCellBase::ExpirePeerRevocationReasons(TInstant deadline)
{
    for (auto& peer : Peers_) {
        if (!peer.LastRevocationReason.IsOK() && peer.LastRevocationReason.GetDatetime() < deadline) {
            peer.LastRevocationReason = {};
        }
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
        const auto* hydraContext = NHydra::GetCurrentHydraContext();
        LastLeaderChangeTime_ = hydraContext->GetTimestamp();
    }
    peer.LastSeenState = peerState;
}

TNode::TCellSlot* TCellBase::FindCellSlot(int peerId) const
{
    auto* node = Peers_[peerId].Node;
    if (!node) {
        return nullptr;
    }

    return node->FindCellSlot(this);
}

NHydra::EPeerState TCellBase::GetPeerState(int peerId) const
{
    auto* slot = FindCellSlot(peerId);
    if (!slot) {
        return NHydra::EPeerState::None;
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
        auto* node = peer.Node;
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
    auto* leaderNode = leaderPeer.Node;
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
        auto* node = peer.Node;
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
    auto* leaderNode = leaderPeer.Node;
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

ECellHealth TCellBase::CombineHealths(ECellHealth lhs, ECellHealth rhs)
{
    static constexpr std::array<ECellHealth, 4> HealthOrder{{
        ECellHealth::Failed,
        ECellHealth::Degraded,
        ECellHealth::Initializing,
        ECellHealth::Good
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
