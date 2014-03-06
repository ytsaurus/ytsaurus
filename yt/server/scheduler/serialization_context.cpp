#include "stdafx.h"
#include "serialization_context.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 13;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 13;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

