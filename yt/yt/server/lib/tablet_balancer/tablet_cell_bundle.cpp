#include "tablet_cell_bundle.h"

#include "private.h"
#include "tablet_cell.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletBalancerLogger;

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
