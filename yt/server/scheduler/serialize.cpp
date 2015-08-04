#include "stdafx.h"
#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 26;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 26;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

