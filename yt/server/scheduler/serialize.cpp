#include "stdafx.h"
#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 29;
}

bool ValidateSnapshotVersion(int version)
{
<<<<<<< HEAD
    return version == GetCurrentSnapshotVersion();
=======
    return version == 29;
>>>>>>> prestable/0.17.4
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

