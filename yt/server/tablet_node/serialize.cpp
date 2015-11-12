#include "serialize.h"
#include "private.h"

#include <yt/core/misc/common.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 9;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 9;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
