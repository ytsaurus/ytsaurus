#include "table_mount_cache.h"

namespace NYT {
namespace NTabletClient {

////////////////////////////////////////////////////////////////////////////////

TTabletInfo& TTabletInfo::operator= (const TTabletInfo& other)
{
    TabletId = other.TabletId;
    MountRevision = other.MountRevision;
    State = other.State;
    PivotKey = other.PivotKey;
    CellId = other.CellId;
    TableId = other.TableId;
    UpdateTime = other.UpdateTime;
    Owners = other.Owners;

    return *this;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
