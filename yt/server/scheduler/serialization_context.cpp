#include "stdafx.h"
#include "serialization_context.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 11;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 11;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

