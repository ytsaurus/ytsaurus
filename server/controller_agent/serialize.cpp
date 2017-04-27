#include "serialize.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200342;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 200342;
}

////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

