#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200005;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 200005;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

