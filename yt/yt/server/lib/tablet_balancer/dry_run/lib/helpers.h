#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>

namespace NYT::NTabletBalancer::NDryRun {

////////////////////////////////////////////////////////////////////////////////

TTabletPtr FindTabletInBundle(const TTabletCellBundlePtr& bundle, TTabletId tabletId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer::NDryRun
