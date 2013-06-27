#include "stdafx.h"
#include "serialization_context.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 5;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 5;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

