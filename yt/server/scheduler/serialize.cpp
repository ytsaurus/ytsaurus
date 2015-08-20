#include "stdafx.h"
#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 25;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 25;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

