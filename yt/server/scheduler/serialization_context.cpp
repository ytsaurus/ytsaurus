#include "stdafx.h"
#include "serialization_context.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 8;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 8;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

