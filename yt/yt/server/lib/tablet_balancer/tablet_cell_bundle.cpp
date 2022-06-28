#include "tablet_cell.h"
#include "tablet_cell_bundle.h"

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

TTabletCellBundle::TTabletCellBundle(TString name)
    : Name(std::move(name))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
