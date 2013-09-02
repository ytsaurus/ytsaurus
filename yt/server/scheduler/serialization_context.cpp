#include "stdafx.h"
#include "serialization_context.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 10;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 10;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

