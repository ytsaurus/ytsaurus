#include "cell_base.h"
#include "cell_bundle.h"

#include <yt/server/master/tablet_server/tablet.h>

#include <yt/server/master/cell_master/serialize.h>

#include <yt/server/master/transaction_server/transaction.h>

#include <yt/server/master/object_server/object.h>

#include <yt/server/lib/cell_server/proto/cell_manager.pb.h>

#include <yt/server/lib/hydra/mutation_context.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NCellServer {

using namespace NCellMaster;
using namespace NHiveClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NTabletClient;
using namespace NYTree;
using namespace NTabletServer;

////////////////////////////////////////////////////////////////////////////////

void TCellStatus::Persist(TPersistenceContext& context)
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

void TCellBase::TPeer::Persist(NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Descriptor);
    Persist(context, Node);
    Persist(context, LastSeenTime);
    if (context.GetVersion() >= EMasterReign::CellPeerRevocationReason) {
        Persist(context, LastRevocationReason);
    }
}

////////////////////////////////////////////////////////////////////////////////

TCellBase::TCellBase(TTamedCellId id)
    : TNonversionedObjectBase(id)
    , LeadingPeerId_(0)
    , ConfigVersion_(0)
    , Config_(New<TTamedCellConfig>())
    , PrerequisiteTransaction_(nullptr)
    , CellBundle_(nullptr)
{ }

void TCellBase::Save(TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, LeadingPeerId_);
    Save(context, Peers_);
    Save(context, ConfigVersion_);
    Save(context, *Config_);
    Save(context, PrerequisiteTransaction_);
    Save(context, CellBundle_);
    Save(context, CellLifeStage_);
    Save(context, GossipStatus_);
    Save(context, PeerCount_);
}

void TCellBase::Load(TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;

    Load(context, LeadingPeerId_);
    Load(context, Peers_);
    Load(context, ConfigVersion_);
    Load(context, *Config_);
    // COMPAT(savrus)
    if (context.GetVersion() < EMasterReign::CellServer) {
        Load(context, CompatTablets_);
        Load<TTabletCellStatistics>(context);
        Load<THashMap<NObjectClient::TCellTag, NTabletServer::TTabletCellStatistics>>(context);
    }
    Load(context, PrerequisiteTransaction_);
    Load(context, CellBundle_);
    Load(context, CellLifeStage_);
    // COMPAT(savrus)
    if (context.GetVersion() >= EMasterReign::CellServer) {
        Load(context, GossipStatus_);
    }
    // COMPAT(gritukan)
    if (context.GetVersion() >= EMasterReign::DynamicPeerCount) {
        Load(context, PeerCount_);
    }
}

TPeerId TCellBase::FindPeerId(const TString& address) const
{
    for (auto peerId = 0; peerId < Peers_.size(); ++peerId) {
        const auto& peer = Peers_[peerId];
        if (peer.Descriptor.GetDefaultAddress() == address) {
            return peerId;
        }
    }
    return InvalidPeerId;
}

TPeerId TCellBase::GetPeerId(const TString& address) const
{
    auto peerId = FindPeerId(address);
    YT_VERIFY(peerId != InvalidPeerId);
    return peerId;
}

TPeerId TCellBase::FindPeerId(TNode* node) const
{
    for (TPeerId peerId = 0; peerId < Peers_.size(); ++peerId) {
        if (Peers_[peerId].Node == node) {
            return peerId;
        }
    }
    return InvalidPeerId;
}

TPeerId TCellBase::GetPeerId(TNode* node) const
{
    auto peerId = FindPeerId(node);
    YT_VERIFY(peerId != InvalidPeerId);
    return peerId;
}

void TCellBase::AssignPeer(const TCellPeerDescriptor& descriptor, TPeerId peerId)
{
    auto& peer = Peers_[peerId];
    YT_VERIFY(peer.Descriptor.IsNull());
    YT_VERIFY(!descriptor.IsNull());
    peer.Descriptor = descriptor;
}

void TCellBase::RevokePeer(TPeerId peerId, const TError& reason)
{
    auto& peer = Peers_[peerId];
    YT_VERIFY(!peer.Descriptor.IsNull());
    peer.Descriptor = TCellPeerDescriptor();
    peer.Node = nullptr;
    peer.LastRevocationReason = NHydra::SanitizeWithCurrentMutationContext(reason);
}

void TCellBase::ExpirePeerRevocationReasons(TInstant deadline)
{
    for (auto& peer : Peers_) {
        if (!peer.LastRevocationReason.IsOK() && peer.LastRevocationReason.GetDatetime() < deadline) {
            peer.LastRevocationReason = {};
        }
    }
}

void TCellBase::AttachPeer(TNode* node, TPeerId peerId)
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

void TCellBase::UpdatePeerSeenTime(TPeerId peerId, TInstant when)
{
    auto& peer = Peers_[peerId];
    peer.LastSeenTime = when;
}

void TCellBase::UpdatePeerState(TPeerId peerId, EPeerState peerState)
{
    auto& peer = Peers_[peerId];
    peer.LastSeenState = peerState;
}

TNode::TCellSlot* TCellBase::FindCellSlot(TPeerId peerId) const
{
    auto* node = Peers_[peerId].Node;
    if (!node) {
        return nullptr;
    }

    return node->FindCellSlot(this);
}

NHydra::EPeerState TCellBase::GetPeerState(NElection::TPeerId peerId) const
{
    auto* slot = FindCellSlot(peerId);
    if (!slot) {
        return NHydra::EPeerState::None;
    }

    return slot->PeerState;
}

ECellHealth TCellBase::GetHealth() const
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

    for (auto peerId = 0; peerId < static_cast<int>(Peers_.size()); ++peerId) {
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

TCellDescriptor TCellBase::GetDescriptor() const
{
    TCellDescriptor descriptor;
    descriptor.CellId = Id_;
    descriptor.ConfigVersion = ConfigVersion_;
    for (auto peerId = 0; peerId < static_cast<int>(Peers_.size()); ++peerId) {
        descriptor.Peers.push_back(TCellPeerDescriptor(
            Peers_[peerId].Descriptor,
            peerId == LeadingPeerId_));
    }
    return descriptor;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
