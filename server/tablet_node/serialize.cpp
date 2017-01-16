#include "serialize.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 100000;
}

bool ValidateSnapshotVersion(int version)
{
    return version == GetCurrentSnapshotVersion();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
