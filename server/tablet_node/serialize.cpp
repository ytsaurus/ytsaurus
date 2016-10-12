#include "serialize.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 16;
}

bool ValidateSnapshotVersion(int version)
{
    return
        version == 13 ||
        version == 14 ||
        version == 15 ||
        version == 16;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
