#include "serialize.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 100007;
}

bool ValidateSnapshotVersion(int version)
{
    return
        version == 100004 ||
        version == 100005 ||
        version == 100006 ||
        version == 100007;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
