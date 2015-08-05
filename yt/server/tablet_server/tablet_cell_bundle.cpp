#include "stdafx.h"
#include "tablet_cell_bundle.h"

#include <yt/ytlib/tablet_client/config.h>

namespace NYT {
namespace NTabletServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

TTabletCellBundle::TTabletCellBundle(const TTabletCellBundleId& id)
    : TNonversionedObjectBase(id)
    , Options_(New<TTabletCellOptions>())
{ }

void TTabletCellBundle::Save(TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, *Options_);
}

void TTabletCellBundle::Load(TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, *Options_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

