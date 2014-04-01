#include "stdafx.h"
#include "serialization_context.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 15;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 15;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

