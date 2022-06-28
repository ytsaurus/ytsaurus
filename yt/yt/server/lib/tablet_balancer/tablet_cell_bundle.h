#pragma once

#include "public.h"

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellBundle final
{
    const TString Name;
    TBundleTabletBalancerConfigPtr Config;
    THashMap<TTabletCellId, TTabletCellPtr> TabletCells;
    THashMap<TTableId, TTablePtr> Tables;

    TTabletCellBundle(TString name);

    std::vector<TTabletCellPtr> GetAliveCells() const;
};

DEFINE_REFCOUNTED_TYPE(TTabletCellBundle)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
