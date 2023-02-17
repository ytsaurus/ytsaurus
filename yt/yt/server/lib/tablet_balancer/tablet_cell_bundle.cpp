#include "tablet_cell.h"
#include "tablet_cell_bundle.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

std::vector<TTabletCellPtr> TTabletCellBundle::GetAliveCells() const
{
    std::vector<TTabletCellPtr> cells;
    for (const auto& [id, cell] : TabletCells) {
        if (cell->IsAlive()) {
            cells.push_back(cell);
        }
    }
    return cells;
}

bool TTabletCellBundle::AreAllCellsAssignedToPeers() const
{
    for (const auto& [id, cell] : TabletCells) {
        if (!cell->NodeAddress.has_value()) {
            return false;
        }
    }
    return true;
}


TTabletCellBundle::TTabletCellBundle(TString name)
    : Name(std::move(name))
{ }

void Deserialize(TTabletCellBundle::TNodeMemoryStatistics& value, NYTree::INodePtr node)
{
    auto mapNode = node->AsMap();
    value.Used = mapNode->GetChildValueOrThrow<i64>("used");

    if (auto limit = mapNode->FindChildValue<i64>("limit")) {
        value.Limit = *limit;
    }
}
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
