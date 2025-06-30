#pragma once

#include "public.h"

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellBundle final
{
    struct TNodeStatistics
    {
        int TabletSlotCount;
        i64 MemoryLimit = std::numeric_limits<i64>::max() / 2;
        i64 MemoryUsed;
    };

    const TString Name;
    TBundleTabletBalancerConfigPtr Config;
    THashMap<TTabletCellId, TTabletCellPtr> TabletCells;
    THashMap<TTableId, TTablePtr> Tables;
    THashMap<TNodeAddress, TNodeStatistics> NodeStatistics;

    NTableClient::TTableSchemaPtr PerformanceCountersTableSchema;
    THashMap<TClusterName, NTableClient::TTableSchemaPtr> PerClusterPerformanceCountersTableSchemas;

    TTabletCellBundle(TString name);

    std::vector<TTabletCellPtr> GetAliveCells() const;
};

DEFINE_REFCOUNTED_TYPE(TTabletCellBundle)

void Deserialize(TTabletCellBundle::TNodeStatistics& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
