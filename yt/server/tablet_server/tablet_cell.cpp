#include "stdafx.h"
#include "tablet_cell.h"
#include "tablet.h"

#include <core/ytree/fluent.h>

#include <ytlib/tablet_client/config.h>

#include <server/transaction_server/transaction.h>

#include <server/object_server/object.h>

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NTabletServer {

using namespace NYTree;
using namespace NHive;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

void TTabletCell::TPeer::Persist(NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Descriptor);
    Persist(context, Node);
    Persist(context, LastSeenTime);
}

////////////////////////////////////////////////////////////////////////////////

TTabletCell::TTabletCell(const TTabletCellId& id)
    : TNonversionedObjectBase(id)
    , Size_(-1)
    , ConfigVersion_(0)
    , Config_(New<TTabletCellConfig>())
    , Options_(New<TTabletCellOptions>())
    , PrerequisiteTransaction_(nullptr)
{ }

void TTabletCell::Save(TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, Size_);
    Save(context, Peers_);
    Save(context, ConfigVersion_);
    Save(context, *Config_);
    Save(context, *Options_);
    Save(context, Tablets_);
    Save(context, TotalStatistics_);
    Save(context, PrerequisiteTransaction_);
}

void TTabletCell::Load(TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, Size_);
    Load(context, Peers_);
    Load(context, ConfigVersion_);
    Load(context, *Config_);
    Load(context, *Options_);
    Load(context, Tablets_);
    Load(context, TotalStatistics_);
    Load(context, PrerequisiteTransaction_);
}

TPeerId TTabletCell::FindPeerId(const Stroka& address) const
{
    for (TPeerId peerId = 0; peerId < Peers_.size(); ++peerId) {
        const auto& peer = Peers_[peerId];
        if (peer.Descriptor.GetDefaultAddress() == address) {
            return peerId;
        }
    }
    return InvalidPeerId;
}

TPeerId TTabletCell::GetPeerId(const Stroka& address) const
{
    auto peerId = FindPeerId(address);
    YCHECK(peerId != InvalidPeerId);
    return peerId;
}

TPeerId TTabletCell::FindPeerId(TNode* node) const
{
    for (TPeerId peerId = 0; peerId < Peers_.size(); ++peerId) {
        if (Peers_[peerId].Node == node) {
            return peerId;
        }
    }
    return InvalidPeerId;
}

TPeerId TTabletCell::GetPeerId(TNode* node) const
{
    auto peerId = FindPeerId(node);
    YCHECK(peerId != InvalidPeerId);
    return peerId;
}

void TTabletCell::AssignPeer(const TNodeDescriptor& descriptor, TPeerId peerId)
{
    auto& peer = Peers_[peerId];
    YCHECK(peer.Descriptor.IsNull());
    YCHECK(!descriptor.IsNull());
    peer.Descriptor = descriptor;
}

void TTabletCell::RevokePeer(TPeerId peerId)
{
    auto& peer = Peers_[peerId];
    YCHECK(!peer.Descriptor.IsNull());
    peer.Descriptor = TNodeDescriptor();
    peer.Node = nullptr;
}

void TTabletCell::AttachPeer(TNode* node, TPeerId peerId)
{
    auto& peer = Peers_[peerId];
    YCHECK(peer.Descriptor.GetDefaultAddress() == node->GetDefaultAddress());

    YCHECK(!peer.Node);
    peer.Node = node;
}

void TTabletCell::DetachPeer(TNode* node)
{
    auto peerId = FindPeerId(node);
    if (peerId != InvalidPeerId) {
        Peers_[peerId].Node = nullptr;
    }
}

void TTabletCell::UpdatePeerSeenTime(TPeerId peerId, TInstant when)
{
    auto& peer = Peers_[peerId];
    peer.LastSeenTime = when;
}

ETabletCellHealth TTabletCell::GetHealth() const
{
    int leaderCount = 0;
    int followerCount = 0;
    for (const auto& peer : Peers_) {
        auto* node = peer.Node;
        if (!IsObjectAlive(node))
            continue;
        const auto* slot = node->GetTabletSlot(this);
        switch (slot->PeerState) {
            case EPeerState::Leading:
                ++leaderCount;
                break;
            case EPeerState::Following:
                ++followerCount;
                break;
            default:
                break;
        }
    }

    if (leaderCount == 1 && followerCount == Size_ - 1) {
        return ETabletCellHealth::Good;
    }

    if (Tablets_.empty()) {
        return ETabletCellHealth::Initializing;
    }

    if (leaderCount == 1 && followerCount >= Size_ / 2) {
        return ETabletCellHealth::Degraded;
    }

    return ETabletCellHealth::Failed;
}

TCellDescriptor TTabletCell::GetDescriptor() const
{
    TCellDescriptor descriptor;
    descriptor.CellId = Id_;
    descriptor.ConfigVersion = ConfigVersion_;
    for (const auto& peer : Peers_) {
        descriptor.Peers.push_back(peer.Descriptor);
    }
    return descriptor;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

