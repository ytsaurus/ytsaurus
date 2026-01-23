#include "tablet_cell_bundle.h"

#include "config.h"
#include "private.h"
#include "table.h"
#include "tablet.h"
#include "tablet_cell.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

std::vector<TTabletCellPtr> TTabletCellBundle::GetAliveCells() const
{
    std::vector<TTabletCellPtr> cells;
    for (const auto& [id, cell] : TabletCells) {
        if (cell->IsAlive()) {
            if (!cell->NodeAddress) {
                YT_LOG_WARNING("Alive cell is not assigned to any node (CellId: %v)",
                    id);
                continue;
            }
            cells.push_back(cell);
        }
    }
    return cells;
}

TTabletCellBundle::TTabletCellBundle(TString name)
    : Name(std::move(name))
{ }

TTabletCellBundlePtr TTabletCellBundle::DeepCopy(bool copyCells, bool copyTabletsAndStatistics) const
{
    // No reason to copy statistics without copying cells.
    YT_VERIFY(copyCells || !copyTabletsAndStatistics);

    auto bundle = New<TTabletCellBundle>(Name);
    bundle->Config = Config;

    if (copyTabletsAndStatistics) {
        bundle->NodeStatistics = NodeStatistics;
    }

    THashMap<TTabletId, TTabletPtr> newTablets;

    for (const auto& [id, table] : Tables) {
        auto newTable = table->Clone(bundle.Get(), copyTabletsAndStatistics);
        EmplaceOrCrash(bundle->Tables, id, newTable);

        if (copyTabletsAndStatistics) {
            for (const auto& tablet : table->Tablets) {
                auto newTablet = tablet->Clone(newTable.Get());
                newTable->Tablets.push_back(newTablet);
                EmplaceOrCrash(newTablets, tablet->Id, newTablet);
                EmplaceOrCrash(bundle->Tablets, tablet->Id, std::move(newTablet));
            }
        }
    }

    if (copyCells) {
        for (const auto& [id, cell] : TabletCells) {
            auto newCell = cell->Clone();
            EmplaceOrCrash(bundle->TabletCells, id, newCell);

            for (const auto& [tabletId, tablet] : cell->Tablets) {
                if (auto it = newTablets.find(tabletId); it != newTablets.end()) {
                    EmplaceOrCrash(newCell->Tablets, tabletId, it->second);
                    it->second->Cell = newCell;
                }
            }
        }
    }

    return bundle;
}

THashSet<TGroupName> TTabletCellBundle::GetBalancingGroups() const
{
    THashSet<TGroupName> groups;
    for (const auto& [id, table] : Tables) {
        if (auto groupName = table->GetBalancingGroup()) {
            groups.insert(*groupName);
        }
    }

    return groups;
}

void Deserialize(TTabletCellBundle::TNodeStatistics& value, NYTree::INodePtr node)
{
    auto mapNode = node->AsMap();
    value.MemoryUsed = mapNode->GetChildValueOrThrow<i64>("used");

    if (auto limit = mapNode->FindChildValue<i64>("limit")) {
        value.MemoryLimit = *limit;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
