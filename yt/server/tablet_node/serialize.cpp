#include "serialize.h"
#include "private.h"

#include <yt/core/misc/common.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 10;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 10;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
