#include "stdafx.h"
#include "tablet.h"
#include "tablet_cell.h"

#include <server/table_server/table_node.h>

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NTabletServer {

using namespace NTableServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TTablet::TTablet(const TTabletId& id)
    : TNonversionedObjectBase(id)
    , State_(ETabletState::Unmounted)
    , Table_(nullptr)
    , Cell_(nullptr)
{ }

void TTablet::Save(TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, State_);
    Save(context, Table_);
    Save(context, Cell_);
    Save(context, PivotKey_);
}

void TTablet::Load(TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, State_);
    Load(context, Table_);
    Load(context, Cell_);
    Load(context, PivotKey_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

