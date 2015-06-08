#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 7;
}

bool ValidateSnapshotVersion(int version)
{
    return
        version == 7;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
