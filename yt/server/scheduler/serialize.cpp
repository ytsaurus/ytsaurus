#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200426;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 200426;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

