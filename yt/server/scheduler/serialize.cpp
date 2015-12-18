#include "serialize.h"

#include <yt/core/misc/common.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 30;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 30;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

