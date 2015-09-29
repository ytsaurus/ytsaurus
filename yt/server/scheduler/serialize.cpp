#include "stdafx.h"
#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 27;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 27;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

