#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 6;
}

bool ValidateSnapshotVersion(int version)
{
    return
        version == 4 ||
        version == 5 ||
        version == 6;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
