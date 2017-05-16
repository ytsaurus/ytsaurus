#include "serialize.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200426;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 200426;
}

////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

