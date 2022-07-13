#pragma once

#include "public.h"

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/core/misc/range.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct TTabletSizeConfig
{
    i64 MinTabletSize = 0;
    i64 MaxTabletSize = 0;
    i64 DesiredTabletSize = 0;
    std::optional<int> MinTabletCount;
};

struct TTabletBalancerContext
{
    THashSet<const TTablet*> TouchedTablets;

    bool IsTabletUntouched(const TTablet* tablet) const;
};

struct TReshardDescriptor
{
    std::vector<TTablet*> Tablets;
    int TabletCount;
    i64 DataSize;
};

struct TTabletMoveDescriptor
{
    TTablet* Tablet;
    TTabletCellId TabletCellId;
};

////////////////////////////////////////////////////////////////////////////////

// These functions can be called in mutation context. Keep them deterministic.

TTabletSizeConfig GetTabletSizeConfig(const NTableServer::TTableNode* table);

i64 GetTabletBalancingSize(TTablet* tablet);

bool IsTabletReshardable(const TTablet* tablet, bool ignoreConfig);

std::vector<TReshardDescriptor> MergeSplitTabletsOfTable(
    TRange<TTablet*> tabletRange,
    TTabletBalancerContext* context);

std::vector<TTabletMoveDescriptor> ReassignInMemoryTablets(
    const TTabletCellBundle* bundle,
    const std::optional<THashSet<const NTableServer::TTableNode*>>& movableTables,
    bool ignoreTableWiseConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
