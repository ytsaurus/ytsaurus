#include "table_registry.h"

#include <yt/yt/server/lib/tablet_balancer/table.h>
#include <yt/yt/server/lib/tablet_balancer/tablet.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell_bundle.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

void TTableRegistry::AddTable(const TTablePtr& table)
{
    if (auto it = Tables_.find(table->Id); it != Tables_.end()) {
        YT_VERIFY(it->second->Bundle != table->Bundle);

        UnlinkTableFromOldBundle(it->second);
        EraseOrCrash(it->second->Bundle->Tables, table->Id);

        it->second = table;
    } else {
        EmplaceOrCrash(Tables_, table->Id, table);
    }
}

void TTableRegistry::RemoveTable(const TTableId& tableId)
{
    auto it = Tables_.find(tableId);
    YT_VERIFY(it != Tables_.end());

    UnlinkTableFromOldBundle(it->second);
    Tables_.erase(it);
}

void TTableRegistry::RemoveBundle(const TTabletCellBundlePtr& bundle)
{
    bundle->TabletCells.clear();
    for (const auto& [tableId, table] : bundle->Tables) {
        table->Tablets.clear();
        RemoveTable(tableId);
    }
    bundle->Tables.clear();
}

void TTableRegistry::AddAlienTablePath(const TClusterName& cluster, const NYPath::TYPath& path, TTableId tableId)
{
    EmplaceOrCrash(AlienTablePaths_, TAlienTableTag{cluster, path}, tableId);
}

void TTableRegistry::AddAlienTable(const TAlienTablePtr& table, const std::vector<TTableId>& majorTableIds)
{
    EmplaceOrCrash(AlienTables_, table->Id, table);
    TablesWithAlienTable_.insert(majorTableIds.begin(), majorTableIds.end());
}

void TTableRegistry::DropAllAlienTables()
{
    for (const auto& tableId : TablesWithAlienTable_) {
        const auto& table = GetOrCrash(Tables_, tableId);
        table->AlienTables.clear();
    }

    AlienTables_.clear();
    AlienTablePaths_.clear();
    TablesWithAlienTable_.clear();
}

void TTableRegistry::UnlinkTableFromOldBundle(const TTablePtr& table)
{
    for (const auto& tablet : table->Tablets) {
        UnlinkTabletFromCell(tablet);
    }
    table->Tablets.clear();
}

void TTableRegistry::UnlinkTabletFromCell(const TTabletPtr& tablet)
{
    if (auto cell = tablet->Cell.Lock()) {
        EraseOrCrash(cell->Tablets, tablet->Id);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
