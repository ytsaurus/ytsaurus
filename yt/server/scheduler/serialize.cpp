#include "stdafx.h"
#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 19;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 19;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

