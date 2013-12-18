#include "stdafx.h"
#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 12;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 12;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

