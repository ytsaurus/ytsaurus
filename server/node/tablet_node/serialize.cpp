#include "serialize.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 100011;
}

bool ValidateSnapshotVersion(int version)
{
    return
        version == 100008 || // aozeritsky
        version == 100009 || // savrus: Save last commit timestamps for all cells.
        version == 100010 || // savrus: Add tablet cell life stage
        version == 100011 || // ifsmirnov: Serialize chunk read range.
        false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
