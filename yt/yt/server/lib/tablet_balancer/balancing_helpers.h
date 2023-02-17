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

////////////////////////////////////////////////////////////////////////////////

bool IsTabletReshardable(const TTabletPtr& tablet, bool ignoreConfig);

i64 GetTabletBalancingSize(const TTabletPtr& tablet);

////////////////////////////////////////////////////////////////////////////////

std::vector<TReshardDescriptor> MergeSplitTabletsOfTable(
    std::vector<TTabletPtr> tablets,
    const NLogging::TLogger& logger = {});

std::vector<TMoveDescriptor> ReassignInMemoryTablets(
    const TTabletCellBundlePtr& bundle,
    const std::optional<THashSet<TTableId>>& movableTables,
    bool ignoreTableWiseConfig,
    const NLogging::TLogger& logger = {});

std::vector<TMoveDescriptor> ReassignOrdinaryTablets(
    const TTabletCellBundlePtr& bundle,
    const std::optional<THashSet<TTableId>>& movableTables,
    const NLogging::TLogger& logger = {});

std::vector<TMoveDescriptor> ReassignTabletsParameterized(
    const TTabletCellBundlePtr& bundle,
    const std::vector<TString>& performanceCountersKeys,
    bool ignoreTableWiseConfig,
    int maxMoveActionCount,
    double deviationThreshold,
    const NLogging::TLogger& logger = {});

////////////////////////////////////////////////////////////////////////////////

void ApplyMoveTabletAction(const TTabletPtr& tablet, const TTabletCellId& cell);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
