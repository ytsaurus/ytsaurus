#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200004;
}

bool ValidateSnapshotVersion(int version)
{
    return
        version == 200003 ||
        version == 200004;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

