#include "serialize.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 100008;
}

bool ValidateSnapshotVersion(int version)
{
    return
        version == 100004 ||
        version == 100005 ||
        version == 100006 ||
        version == 100007 ||
        version == 100008; // aozeritsky
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
