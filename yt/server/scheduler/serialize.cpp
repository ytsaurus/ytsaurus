#include "stdafx.h"
#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 24;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 24;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

