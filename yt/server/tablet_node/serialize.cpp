#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 8;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 8;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
