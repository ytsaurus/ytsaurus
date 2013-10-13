#include "stdafx.h"
#include "tablet.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

using namespace NHydra;

TTablet::TTablet(const TTabletId& id)
    : Id_(id)
{ }

void TTablet::Save(TSaveContext& context) const
{
    using NYT::Save;
}

void TTablet::Load(TLoadContext& context)
{
    using NYT::Load;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

