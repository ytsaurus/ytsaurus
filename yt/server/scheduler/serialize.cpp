#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200300;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 200300;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

