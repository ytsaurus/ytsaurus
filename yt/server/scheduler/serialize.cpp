#include "stdafx.h"
#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 23;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 23;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

