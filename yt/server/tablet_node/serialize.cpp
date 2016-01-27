#include "serialize.h"
#include "private.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 11;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 10 ||
        version == 11;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
