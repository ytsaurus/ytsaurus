#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200101;
}

bool ValidateSnapshotVersion(int version)
{
    return version == GetCurrentSnapshotVersion();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

