#include "tablet_cell.h"
#include "tablet_cell_bundle.h"
#include "tablet.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NCellServer;
using namespace NHiveClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

std::string TTabletCell::GetLowercaseObjectName() const
{
    return Format("tablet cell %v", GetId());
}

std::string TTabletCell::GetCapitalizedObjectName() const
{
    return Format("Tablet cell %v", GetId());
}

TYPath TTabletCell::GetObjectPath() const
{
    return Format("//sys/tablet_cells/%v", GetId());
}

void TTabletCell::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TCellBase>();

    PHOENIX_REGISTER_FIELD(1, Tablets_);
    PHOENIX_REGISTER_FIELD(2, GossipStatistics_);
}

PHOENIX_DEFINE_TYPE(TTabletCell);

PHOENIX_DEFINE_TEMPLATE_TYPE(NCellMaster::TGossipValue, (TTabletCellStatistics));

TTabletCellBundleRawPtr TTabletCell::GetTabletCellBundle() const
{
    const auto& cellBundle = CellBundle();
    YT_VERIFY(cellBundle->GetType() == EObjectType::TabletCellBundle);
    return cellBundle->As<TTabletCellBundle>();
}

void TTabletCell::RecomputeClusterStatistics()
{
    GossipStatistics().Cluster() = TTabletCellStatistics();
    for (const auto& [cellTag, statistics] : GossipStatistics().Multicell()) {
        GossipStatistics().Cluster() += statistics;
    }
}

TCellDescriptor TTabletCell::GetDescriptor() const
{
    TCellDescriptor descriptor;
    descriptor.CellId = Id_;
    descriptor.ConfigVersion = ConfigVersion_;
    for (int peerId = 0; peerId < std::ssize(Peers_); ++peerId) {
        descriptor.Peers.push_back(TCellPeerDescriptor(
            Peers_[peerId].Descriptor,
            peerId == LeadingPeerId_));
    }
    return descriptor;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

