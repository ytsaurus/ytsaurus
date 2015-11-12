#include "serialize.h"

#include <yt/core/misc/common.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 29;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 29;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

