#include "stdafx.h"
#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 21;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 21;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

