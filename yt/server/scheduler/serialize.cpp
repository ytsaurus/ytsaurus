#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200008;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 200007 || version == 200008;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

