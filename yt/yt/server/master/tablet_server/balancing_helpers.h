#pragma once

#include "public.h"

#include <yt/server/master/table_server/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct TTabletSizeConfig
{
    i64 MinTabletSize;
    i64 MaxTabletSize;
    i64 DesiredTabletSize;
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

TTabletSizeConfig GetTabletSizeConfig(TTablet* tablet);

i64 GetTabletBalancingSize(TTablet* tablet, const TTabletManagerPtr& tabletManager);

bool IsTabletReshardable(const TTablet* tablet, bool ignoreConfig);

std::vector<TReshardDescriptor> MergeSplitTablet(
    TTablet* tablet,
    const TTabletSizeConfig& bounds,
    TTabletBalancerContext* context,
    const TTabletManagerPtr& tabletManager);

std::vector<TTabletMoveDescriptor> ReassignInMemoryTablets(
    const TTabletCellBundle* bundle,
    const std::optional<THashSet<const NTableServer::TTableNode*>>& movableTables,
    bool ignoreTableWiseConfig,
    TTabletBalancerContext* context,
    const TTabletManagerPtr& tabletManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
