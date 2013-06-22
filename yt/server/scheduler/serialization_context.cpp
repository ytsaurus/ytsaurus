#include "stdafx.h"
#include "serialization_context.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 4;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 4;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

