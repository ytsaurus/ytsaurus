#include "serialize.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 100001;
}

bool ValidateSnapshotVersion(int version)
{
    return
        version == 100000 ||
        version == 100001;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
