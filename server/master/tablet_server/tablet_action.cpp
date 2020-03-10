#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"
#include "tablet_action.h"

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NTableClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

TTabletAction::TTabletAction(TTabletActionId id)
    : TNonversionedObjectBase(id)
{ }

TString TTabletAction::GetLowercaseObjectName() const
{
    return Format("tablet action %v", GetId());
}

TString TTabletAction::GetCapitalizedObjectName() const
{
    return Format("Tablet action %v", GetId());
}

void TTabletAction::Save(NCellMaster::TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, Kind_);
    Save(context, State_);
    Save(context, Tablets_);
    Save(context, TabletCells_);
    Save(context, PivotKeys_);
    Save(context, TabletCount_);
    Save(context, SkipFreezing_);
    Save(context, Freeze_);
    Save(context, Error_);
    Save(context, CorrelationId_);
    Save(context, ExpirationTime_);
    Save(context, TabletCellBundle_);
}

void TTabletAction::Load(NCellMaster::TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, Kind_);
    Load(context, State_);
    Load(context, Tablets_);
    Load(context, TabletCells_);
    Load(context, PivotKeys_);
    Load(context, TabletCount_);
    Load(context, SkipFreezing_);
    Load(context, Freeze_);
    Load(context, Error_);
    Load(context, CorrelationId_);
    Load(context, ExpirationTime_);
    Load(context, TabletCellBundle_);
}

bool TTabletAction::IsFinished() const
{
    return State_ == ETabletActionState::Completed || State_ == ETabletActionState::Failed;
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TTabletAction& action)
{
    return Format("ActionId: %v, State: %v, Kind: %v, SkipFreezing: %v, Freeze: %v, TabletCount: %v, Tablets: %v, "
        "Cells: %v, PivotKeys: %v, TabletBalancerCorrelationId: %v, ExpirationTime: %v",
        action.GetId(),
        action.GetState(),
        action.GetKind(),
        action.GetSkipFreezing(),
        action.GetFreeze(),
        action.GetTabletCount(),
        MakeFormattableView(action.Tablets(), TObjectIdFormatter()),
        MakeFormattableView(action.TabletCells(), TObjectIdFormatter()),
        action.PivotKeys(),
        action.GetCorrelationId(),
        action.GetExpirationTime());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
