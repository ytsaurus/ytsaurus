#include "serialize.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 100006;
}

bool ValidateSnapshotVersion(int version)
{
    return
        version == 100004 ||
        version == 100005 ||
        version == 100006;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
