#include "stdafx.h"
#include "tablet.h"
#include "tablet_cell.h"

#include <server/table_server/table_node.h>

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NTabletServer {

using namespace NTableServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TTablet::TTablet(const TTabletId& id)
    : TNonversionedObjectBase(id)
    , State_(ETabletState::Initializing)
    , Table_(nullptr)
    , Cell_(nullptr)
{ }

void TTablet::Save(TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, State_);
    SaveObjectRef(context, Table_);
    SaveObjectRef(context, Cell_);
}

void TTablet::Load(TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, State_);
    LoadObjectRef(context, Table_);
    LoadObjectRef(context, Cell_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

