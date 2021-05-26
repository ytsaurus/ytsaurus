#include "chaos_cell.h"
#include "chaos_cell_bundle.h"
#include "config.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/ytlib/tablet_client/public.h>
#include <yt/yt/ytlib/tablet_client/config.h>

namespace NYT::NChaosServer {

using namespace NCellMaster;
using namespace NCellServer;
using namespace NHiveClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TChaosCell::Save(TSaveContext& context) const
{
    TCellBase::Save(context);

    using NYT::Save;
}

void TChaosCell::Load(TLoadContext& context)
{
    TCellBase::Load(context);

    using NYT::Load;
}

TChaosCellBundle* TChaosCell::GetChaosCellBundle() const
{
    auto* cellBundle = GetCellBundle();
    YT_VERIFY(cellBundle->GetType() == EObjectType::ChaosCellBundle);
    return cellBundle->As<TChaosCellBundle>();
}

bool TChaosCell::IsAlienPeer(int peerId) const
{
    const auto& options = GetChaosCellBundle()->GetChaosOptions();
    return options->Peers[peerId]->AlienCluster.has_value();
}

TCellDescriptor TChaosCell::GetDescriptor() const
{
    TCellDescriptor descriptor;
    descriptor.CellId = Id_;
    descriptor.ConfigVersion = ConfigVersion_;
    const auto& chaosOptions = GetCellBundle()->As<TChaosCellBundle>()->GetChaosOptions();
    for (int peerId = 0; peerId < std::ssize(chaosOptions->Peers); ++peerId) {
        if (IsAlienPeer(peerId)) {
            TCellPeerDescriptor peerDescriptor;
            peerDescriptor.SetVoting(true);
            peerDescriptor.SetAlienCluster(*chaosOptions->Peers[peerId]->AlienCluster);
            descriptor.Peers.push_back(std::move(peerDescriptor));
        } else {
            descriptor.Peers.push_back(TCellPeerDescriptor(
                Peers_[peerId].Descriptor,
                true));
        }
    }
    return descriptor;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
