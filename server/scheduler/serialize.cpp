#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200384;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 200384;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

