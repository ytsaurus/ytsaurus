#include "serialize.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 100009;
}

bool ValidateSnapshotVersion(int version)
{
    return
        version == 100008 || // aozeritsky
        version == 100009;   // savrus: Save last commit timestamps for all cells.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
