#include "tablet_cell.h"
#include "tablet_cell_bundle.h"
#include "tablet.h"

#include <yt/server/master/cell_master/serialize.h>

#include <yt/server/master/transaction_server/transaction.h>

#include <yt/server/master/object_server/object.h>

#include <yt/ytlib/tablet_client/config.h>

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

TTabletCell::TTabletCell(TTabletCellId id)
    : TCellBase(id)
{ }

TString TTabletCell::GetLowercaseObjectName() const
{
    return Format("tablet cell %v", GetId());
}

TString TTabletCell::GetCapitalizedObjectName() const
{
    return Format("Tablet cell %v", GetId());
}

void TTabletCell::Save(TSaveContext& context) const
{
    TCellBase::Save(context);

    using NYT::Save;
    Save(context, Tablets_);
    Save(context, GossipStatistics_);
}

void TTabletCell::Load(TLoadContext& context)
{
    TCellBase::Load(context);

    using NYT::Load;
    // COMPAT(savrus)
    if (context.GetVersion() < EMasterReign::CellServer) {
        Tablets_ = std::move(CompatTablets_);
        return;
    }

    Load(context, Tablets_);
    Load(context, GossipStatistics_);
}

TTabletCellBundle* TTabletCell::GetTabletCellBundle() const
{
    auto* cellBundle = GetCellBundle();
    YT_VERIFY(cellBundle->GetType() == EObjectType::TabletCellBundle);
    return cellBundle->As<TTabletCellBundle>();
}

void TTabletCell::RecomputeClusterStatistics()
{
    GossipStatistics().Cluster() = TTabletCellStatistics();
    GossipStatistics().Cluster().Decommissioned = true;
    GossipStatistics().Cluster().Health = GetHealth();
    for (const auto& [cellTag, statistics] : GossipStatistics().Multicell()) {
        GossipStatistics().Cluster() += statistics;
        GossipStatistics().Cluster().Decommissioned &= statistics.Decommissioned;
        GossipStatistics().Cluster().Health = CombineHealths(GossipStatistics().Cluster().Health, statistics.Health);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

