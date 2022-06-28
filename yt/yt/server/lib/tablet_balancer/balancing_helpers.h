#pragma once

#include "public.h"
#include "tablet.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/range.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TReshardDescriptor
{
    std::vector<TTabletId> Tablets;
    int TabletCount;
    i64 DataSize;
};

struct TMoveDescriptor
{
    TTabletId TabletId;
    TTabletCellId TabletCellId;
};

struct TTabletBalancerContext
{
    THashSet<TTabletId> TouchedTablets;

    bool IsTabletUntouched(TTabletId tabletId) const;
};

////////////////////////////////////////////////////////////////////////////////

bool IsTabletReshardable(const TTabletPtr tablet, bool ignoreConfig);

std::vector<TReshardDescriptor> MergeSplitTabletsOfTable(
    TRange<TTabletPtr> tabletRange,
    TTabletBalancerContext* context,
    const NLogging::TLogger& logger);

std::vector<TMoveDescriptor> ReassignInMemoryTablets(
    const TTabletCellBundlePtr& bundle,
    const std::optional<THashSet<TTableId>>& movableTables,
    bool ignoreTableWiseConfig,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
