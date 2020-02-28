#include "config.h"
#include "private.h"
#include "tablet_cell_bundle.h"
#include "tablet_cell.h"

#include <yt/ytlib/tablet_client/config.h>

#include <yt/core/profiling/profile_manager.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NTabletClient;
using namespace NChunkClient;
using namespace NYson;
using namespace NYTree;
using namespace NCellServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

TTabletCellBundle::TTabletCellBundle(TTabletCellBundleId id)
    : TCellBundle(id)
    , TabletBalancerConfig_(New<TTabletBalancerConfig>())
{ }

void TTabletCellBundle::IncreaseActiveTabletActionCount()
{
    ++ActiveTabletActionCount_;
}

void TTabletCellBundle::DecreaseActiveTabletActionCount()
{
    YT_LOG_ERROR_UNLESS(ActiveTabletActionCount_ > 0,
        "Attempting to decrease non-positive ActiveTabletActionCount "
        "(ActiveTabletActionCount: %v, Bundle: %v)",
        ActiveTabletActionCount_,
        GetName());
    --ActiveTabletActionCount_;
}

std::vector<const TTabletCell*> TTabletCellBundle::GetAliveCells() const
{
    std::vector<const TTabletCell*> cells;
    for (const auto* cell : Cells()) {
        if (IsObjectAlive(cell) && !cell->IsDecommissionStarted() && cell->GetCellBundle() == this) {
            YT_VERIFY(cell->GetType() == EObjectType::TabletCell);
            cells.push_back(cell->As<TTabletCell>());
        }
    }
    return cells;
}

TString TTabletCellBundle::GetLowercaseObjectName() const
{
    return Format("tablet cell bundle %Qv", GetName());
}

TString TTabletCellBundle::GetCapitalizedObjectName() const
{
    return Format("Tablet cell bundle %Qv", GetName());
}

void TTabletCellBundle::Save(TSaveContext& context) const
{
    TCellBundle::Save(context);

    using NYT::Save;
    Save(context, *TabletBalancerConfig_);
}

void TTabletCellBundle::Load(TLoadContext& context)
{
    TCellBundle::Load(context);

    using NYT::Load;

    // COMPAT(savrus)
    if (context.GetVersion() >= EMasterReign::CellServer) {
        Load(context, *TabletBalancerConfig_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

