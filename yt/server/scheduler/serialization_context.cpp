#include "stdafx.h"
#include "serialization_context.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

bool ValidateSnapshotVersion(int version)
{
    return version == 2;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

