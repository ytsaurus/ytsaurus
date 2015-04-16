#include "stdafx.h"
#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 22;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 22;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

