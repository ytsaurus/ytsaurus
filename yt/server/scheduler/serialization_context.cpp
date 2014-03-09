#include "stdafx.h"
#include "serialization_context.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 14;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 14;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

