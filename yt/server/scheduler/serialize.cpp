#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200201;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 200201;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

