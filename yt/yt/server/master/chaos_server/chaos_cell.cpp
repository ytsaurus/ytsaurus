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
    Save(context, AlienConfigVersions_);
    Save(context, CumulativeAlienConfigVersion_);
}

void TChaosCell::Load(TLoadContext& context)
{
    TCellBase::Load(context);

    using NYT::Load;
    // COMPAT(savrus)
    if (context.GetVersion() >= EMasterReign::SyncAlienCells) {
        Load(context, AlienConfigVersions_);
        Load(context, CumulativeAlienConfigVersion_);
    }
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

void TChaosCell::UpdateAlienPeer(TPeerId peerId, const NNodeTrackerClient::TNodeDescriptor& descriptor)
{
    YT_VERIFY(IsAlienPeer(peerId));
    Peers_[peerId].Descriptor = descriptor;
}

TCellDescriptor TChaosCell::GetDescriptor() const
{
    TCellDescriptor descriptor;
    descriptor.CellId = Id_;
    // TODO(savrus) descriptor version is used for both cell directory and peer reconfiguration.
    // Need to differentiate them to avoid peer reconfiguration when alien peer is updated.
    descriptor.ConfigVersion = GetDescriptorConfigVersion();
    const auto& chaosOptions = GetCellBundle()->As<TChaosCellBundle>()->GetChaosOptions();
    for (int peerId = 0; peerId < std::ssize(chaosOptions->Peers); ++peerId) {
        auto peerDescriptor = TCellPeerDescriptor(Peers_[peerId].Descriptor, true);
        if (IsAlienPeer(peerId)) {
            peerDescriptor.SetAlienCluster(*chaosOptions->Peers[peerId]->AlienCluster);
        }
        descriptor.Peers.push_back(std::move(peerDescriptor));
    }
    return descriptor;
}

int TChaosCell::GetDescriptorConfigVersion() const
{
    return ConfigVersion_ + CumulativeAlienConfigVersion_;
}

int TChaosCell::GetAlienConfigVersion(int alienClusterIndex) const
{
    return GetOrCrash(AlienConfigVersions_, alienClusterIndex);
}

void TChaosCell::SetAlienConfigVersion(int alienClusterIndex, int version)
{
    AlienConfigVersions_[alienClusterIndex] = version;
    CumulativeAlienConfigVersion_ += 1;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
