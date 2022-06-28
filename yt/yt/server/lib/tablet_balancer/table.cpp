
#include "config.h"
#include "table.h"

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

TTable::TTable(
    bool sorted,
    NYPath::TYPath path,
    NObjectClient::TCellTag cellTag,
    TTableId tableId,
    TTabletCellBundle* bundle)
    : Sorted(sorted)
    , Path(std::move(path))
    , ExternalCellTag(cellTag)
    , Bundle(std::move(bundle))
    , Id(tableId)
{ }

bool TTable::IsBalancingAllowed() const
{
    return Dynamic && TableConfig->EnableAutoTabletMove;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
