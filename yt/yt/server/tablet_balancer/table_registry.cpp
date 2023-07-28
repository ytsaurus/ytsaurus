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
        EraseOrCrash(table->Bundle->Tables, table->Id);

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
