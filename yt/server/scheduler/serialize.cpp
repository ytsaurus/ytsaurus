#include "stdafx.h"
#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 20;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 20;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

