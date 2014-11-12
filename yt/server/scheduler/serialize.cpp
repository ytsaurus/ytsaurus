#include "stdafx.h"
#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 17;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 17;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

