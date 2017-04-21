#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200342;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 200342;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

