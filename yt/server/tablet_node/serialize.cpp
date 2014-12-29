#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 4;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 4;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
