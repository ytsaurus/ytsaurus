#include "stdafx.h"
#include "serialization_context.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 7;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 7;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

