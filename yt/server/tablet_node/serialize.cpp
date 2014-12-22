#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 3;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 3;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
