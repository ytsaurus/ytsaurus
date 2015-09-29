#include "stdafx.h"
#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 28;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 28;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

