#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_action.h"

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NTableClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

TTabletAction::TTabletAction(const TTabletActionId& id)
    : TNonversionedObjectBase(id)
{ }

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
    Save(context, KeepFinished_);
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
    // COMPAT(savrus)
    if (context.GetVersion() >= 600) {
        Load(context, SkipFreezing_);
        Load(context, Freeze_);
    }
    Load(context, Error_);
    Load(context, KeepFinished_);
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TTabletAction& action)
{
    return Format("ActionId: %v, State: %v, Kind: %v, KeepFinished: %v, SkipFreezing: %v, Freeze: %v, TabletCount: %v, Tablets: %v, Cells: %v, PivotKeys: %v",
        action.GetId(),
        action.GetState(),
        action.GetKind(),
        action.GetKeepFinished(),
        action.GetSkipFreezing(),
        action.GetFreeze(),
        action.GetTabletCount(),
        MakeFormattableRange(action.Tablets(), TObjectIdFormatter()),
        MakeFormattableRange(action.TabletCells(), TObjectIdFormatter()),
        action.PivotKeys());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

