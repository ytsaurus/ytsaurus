#include "stdafx.h"
#include "serialization_context.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 6;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 6;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

