#include "stdafx.h"
#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 18;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 18;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

