#include "serialize.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 100010;
}

bool ValidateSnapshotVersion(int version)
{
    return
        version == 100008 || // aozeritsky
        version == 100009 || // savrus: Save last commit timestamps for all cells.
        version == 100010;   // savrus: Add tablet cell life stage
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
