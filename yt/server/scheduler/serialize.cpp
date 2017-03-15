#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200006;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 200006;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

