#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellBundle final
{
    struct TNodeMemoryStatistics
    {
        i64 Limit = std::numeric_limits<i64>::max() / 2;
        i64 Used;
    };

    const TString Name;
    TBundleTabletBalancerConfigPtr Config;
    THashMap<TTabletCellId, TTabletCellPtr> TabletCells;
    THashMap<TTableId, TTablePtr> Tables;
    THashMap<TNodeAddress, TNodeMemoryStatistics> NodeMemoryStatistics;

    TTabletCellBundle(TString name);

    std::vector<TTabletCellPtr> GetAliveCells() const;

    bool AreAllCellsAssignedToPeers() const;
};

DEFINE_REFCOUNTED_TYPE(TTabletCellBundle)

void Deserialize(TTabletCellBundle::TNodeMemoryStatistics& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
