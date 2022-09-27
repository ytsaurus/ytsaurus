#include "tablet.h"

#include <yt/yt/core/ytree/node.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

TTablet::TTablet(
    TTabletId tabletId,
    TTable* table)
    : Id(tabletId)
    , Table(std::move(table))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
