#include "stdafx.h"
#include "serialization_context.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 16;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 16;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

