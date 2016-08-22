#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 48;
}

bool ValidateSnapshotVersion(int version)
{
    return version == GetCurrentSnapshotVersion();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

