#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200007;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 200006 || version == 200007;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

