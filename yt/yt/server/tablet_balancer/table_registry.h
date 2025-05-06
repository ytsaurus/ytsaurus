#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>

#include <yt/yt/core/misc/property.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

class TTableRegistry final
{
public:
    using TTableMap = THashMap<TTableId, TTablePtr>;

    using TAlienTableTag = std::tuple<TString, NYPath::TYPath>;
    using TAlienTablePathMap = THashMap<TAlienTableTag, TTableId>;
    using TAlienTableMap = THashMap<TTableId, TAlienTablePtr>;

    DEFINE_BYREF_RO_PROPERTY(TTableMap, Tables);
    DEFINE_BYREF_RO_PROPERTY(TAlienTablePathMap, AlienTablePaths);
    DEFINE_BYREF_RO_PROPERTY(TAlienTableMap, AlienTables);

public:
    void AddTable(const TTablePtr& table);
    void RemoveTable(const TTableId& tableId);
    void RemoveBundle(const TTabletCellBundlePtr& bundle);

    void AddAlienTablePath(const TClusterName& cluster, const NYPath::TYPath& path, TTableId tableId);
    void AddAlienTable(const TAlienTablePtr& table, const std::vector<TTableId>& majorTableIds);
    void DropAllAlienTables();

private:
    THashSet<TTableId> TablesWithAlienTable_;

    void UnlinkTableFromOldBundle(const TTablePtr& table);
    void UnlinkTabletFromCell(const TTabletPtr& tablet);
};

DEFINE_REFCOUNTED_TYPE(TTableRegistry)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
