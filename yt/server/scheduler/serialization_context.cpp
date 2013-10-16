#include "stdafx.h"
#include "serialization_context.h"

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

